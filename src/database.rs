use std::collections::{HashMap, VecDeque};

use crate::{
    commands::BitOperation,
    common::{
        bitcount, current_time_ms, decode_geohash, encode_geohash, geohash_get_distance,
        CompleteStreamEntryID, KeyValuePair, PatternMatcher, SortedSet, StreamEntryID, MAX_LAT,
        MAX_LON, MIN_LAT, MIN_LON,
    },
};

fn resolve_start_index(start: i64, len: usize) -> usize {
    if start < 0 {
        (start + len as i64).max(0) as usize
    } else {
        start as usize
    }
}

fn resolve_end_index(end: i64, len: usize) -> usize {
    if end < 0 {
        (end + len as i64).max(0) as usize
    } else {
        end as usize
    }
}

#[derive(Default)]
struct ValueEntry {
    value: Vec<u8>,
    expiry_timestamp_ms: Option<u128>,
}

impl ValueEntry {
    fn new() -> Self {
        Self {
            value: vec![],
            expiry_timestamp_ms: None,
        }
    }

    pub(crate) fn to_string(&self) -> Result<String, String> {
        String::from_utf8(self.value.clone())
            .map_err(|_| "Cannot convert bytes to valid string".to_string())
    }

    pub(crate) fn set_from_str(&mut self, s: &str) {
        self.value = s.as_bytes().to_vec();
    }
}

pub(crate) type KeyValuePairList = Vec<KeyValuePair>;

#[derive(Clone)]
pub(crate) struct StreamValue {
    pub(crate) id: CompleteStreamEntryID,
    pub(crate) kvpairs: KeyValuePairList,
}

impl StreamValue {
    fn new(id: CompleteStreamEntryID, kvpairs: KeyValuePairList) -> Self {
        Self { id, kvpairs }
    }
}

pub(crate) type StreamEntry = Vec<StreamValue>;

enum Entry {
    Value(ValueEntry),
    Array(VecDeque<String>),
    Stream(StreamEntry),
    SortedSet(SortedSet),
}

impl Entry {
    fn is_value(&self) -> bool {
        match self {
            Entry::Value(_) => true,
            _ => false,
        }
    }

    fn is_array(&self) -> bool {
        match self {
            Entry::Array(_) => true,
            _ => false,
        }
    }

    fn is_stream(&self) -> bool {
        match self {
            Entry::Stream(_) => true,
            _ => false,
        }
    }

    fn is_set(&self) -> bool {
        match self {
            Entry::SortedSet(_) => true,
            _ => false,
        }
    }

    fn type_name(&self) -> &str {
        match self {
            Entry::Array(_) => "list",
            Entry::Value(_) => "string",
            Entry::Stream(_) => "stream",
            Entry::SortedSet(_) => "sorted set",
        }
    }
}

pub(crate) struct Database {
    dict: HashMap<String, Entry>,
}

impl Database {
    pub(crate) fn new() -> Self {
        Self {
            dict: HashMap::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.dict.clear();
    }

    pub(crate) fn set(
        &mut self,
        key: String,
        value: String,
        expiry_ms: Option<u128>, /* Absolute value. */
    ) -> Result<(), String> {
        self.assert_single_value(&key)?;

        self.dict
            .entry(key)
            .and_modify(|entry| match entry {
                Entry::Value(value_entry) => {
                    value_entry.set_from_str(&value);
                    value_entry.expiry_timestamp_ms = expiry_ms;
                }
                _ => unreachable!(),
            })
            .or_insert(Entry::Value(ValueEntry {
                value: value.into_bytes(),
                expiry_timestamp_ms: expiry_ms,
            }));

        Ok(())
    }

    pub(crate) fn get(&self, key: &String) -> Result<Option<String>, String> {
        self.assert_single_value(key)?;

        Ok(self.dict.get(key).and_then(|entry| {
            let Entry::Value(value_entry) = entry else {
                unreachable!();
            };

            if let Some(expiry_timestamp_ms) = value_entry.expiry_timestamp_ms {
                if expiry_timestamp_ms >= current_time_ms() {
                    Some(value_entry.to_string().ok()?)
                } else {
                    None
                }
            } else {
                Some(value_entry.to_string().ok()?)
            }
        }))
    }

    pub(crate) fn set_bit(&mut self, key: &str, bit: usize, value: u8) -> Result<u8, String> {
        if self.dict.contains_key(key) {
            self.assert_single_value(key)?;
        } else {
            self.dict
                .insert(key.to_owned(), Entry::Value(ValueEntry::new()));
        }

        let entry = self
            .dict
            .get_mut(key)
            .and_then(|entry| {
                let Entry::Value(entry) = entry else {
                    unreachable!();
                };

                Some(entry)
            })
            .unwrap();

        let u8_index = bit / 8;
        while entry.value.len() <= u8_index {
            entry.value.push(0);
        }

        let bit_i = 7 - (bit % 8);
        let old_u8 = entry.value[u8_index];
        let old_value = (old_u8 >> bit_i) & 1;

        entry.value[u8_index] =
            (old_u8 & !((1u8 << bit_i) as u8)) | ((value as u8) << (bit_i as u8));

        Ok(old_value as u8)
    }

    pub(crate) fn get_bit(&self, key: &str, bit: usize) -> Result<u8, String> {
        if self.dict.contains_key(key) {
            self.assert_single_value(key)?;
        } else {
            return Ok(0);
        }

        let entry = self
            .dict
            .get(key)
            .and_then(|entry| {
                let Entry::Value(entry) = entry else {
                    unreachable!();
                };

                Some(entry)
            })
            .unwrap();

        let u8_index = bit / 8;
        if entry.value.len() <= u8_index {
            return Ok(0);
        }

        let bit_i = 7 - (bit % 8);
        let old_u8 = entry.value[u8_index];
        let old_value = (old_u8 >> bit_i) & 1;

        Ok(old_value as u8)
    }

    pub(crate) fn strlen(&self, key: &str) -> Result<usize, String> {
        if self.dict.contains_key(key) {
            self.assert_single_value(key)?;
        } else {
            return Ok(0);
        }

        let entry = self
            .dict
            .get(key)
            .and_then(|entry| {
                let Entry::Value(entry) = entry else {
                    unreachable!();
                };

                Some(entry)
            })
            .unwrap();

        return Ok(entry.value.len());
    }

    pub(crate) fn bitcount(
        &self,
        key: &str,
        start_byte: usize,
        end_byte: usize,
    ) -> Result<usize, String> {
        if self.dict.contains_key(key) {
            self.assert_single_value(key)?;
        } else {
            return Ok(0);
        }

        let entry = self
            .dict
            .get(key)
            .and_then(|entry| {
                let Entry::Value(entry) = entry else {
                    unreachable!();
                };

                Some(entry)
            })
            .unwrap();

        let mut total = 0;
        for byte in &entry.value[start_byte..=end_byte.min(entry.value.len() - 1)] {
            total += bitcount(*byte);
        }

        return Ok(total);
    }

    pub(crate) fn bitop(
        &mut self,
        op: &BitOperation,
        dest_key: &str,
        src_lhs_key: &str,
        src_rhs_key: &str,
    ) -> Result<usize, String> {
        self.assert_single_value(dest_key)?;

        let lhs_bytes = if self.dict.contains_key(src_lhs_key) {
            self.assert_single_value(src_lhs_key)?;
            self.dict
                .get(src_lhs_key)
                .and_then(|entry| {
                    let Entry::Value(entry) = entry else {
                        unreachable!();
                    };

                    Some(entry.value.clone())
                })
                .unwrap()
        } else {
            vec![]
        };

        let rhs_bytes = if self.dict.contains_key(src_rhs_key) {
            self.assert_single_value(src_rhs_key)?;
            self.dict
                .get(src_rhs_key)
                .and_then(|entry| {
                    let Entry::Value(entry) = entry else {
                        unreachable!();
                    };

                    Some(entry.value.clone())
                })
                .unwrap()
        } else {
            vec![]
        };

        let mut result = vec![];
        for i in 0..lhs_bytes.len().max(rhs_bytes.len()) {
            let lhs_byte = if i < lhs_bytes.len() { lhs_bytes[i] } else { 0 };
            let rhs_byte = if i < rhs_bytes.len() { rhs_bytes[i] } else { 0 };
            result.push(match op {
                BitOperation::And => lhs_byte & rhs_byte,
                BitOperation::Or => lhs_byte | rhs_byte,
            });
        }

        let result_bytes_len = result.len();
        if !result.is_empty() {
            let entry = self
                .dict
                .entry(dest_key.to_string())
                .or_insert(Entry::Value(ValueEntry::default()));

            match entry {
                Entry::Value(value_entry) => value_entry.value = result,
                _ => unreachable!(),
            }
        }

        return Ok(result_bytes_len);
    }

    pub(crate) fn push_to_array(
        &mut self,
        key: String,
        values: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_array(&key)?;

        let entry = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::Array(VecDeque::new()));
        let Entry::Array(array) = entry else {
            unreachable!();
        };

        for value in values {
            array.push_back(value);
        }

        Ok(array.len())
    }

    pub(crate) fn insert_to_array(
        &mut self,
        key: String,
        values: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_array(&key)?;

        let entry = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::Array(VecDeque::new()));
        let Entry::Array(array) = entry else {
            unreachable!();
        };

        for value in values {
            array.push_front(value);
        }

        Ok(array.len())
    }

    pub(crate) fn get_list_lrange(
        &self,
        key: &String,
        start: i64,
        end: i64,
    ) -> Result<Vec<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(vec![]);
        }

        let Entry::Array(array) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        let start = resolve_start_index(start, array.len());
        let end = resolve_end_index(end, array.len());
        let mut out = vec![];
        for i in start..=end {
            if i >= array.len() {
                break;
            }

            out.push(array[i as usize].clone());
        }

        Ok(out)
    }

    pub(crate) fn list_length(&self, key: &str) -> Result<usize, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(0);
        }

        let Entry::Array(array) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(array.len())
    }

    pub(crate) fn list_pop_one_front(&mut self, key: &str) -> Result<Option<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        match array.pop_front() {
            Some(elem) => Ok(Some(elem)),
            _ => Ok(None),
        }
    }

    pub(crate) fn list_pop_one_back(&mut self, key: &str) -> Result<Option<String>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        match array.pop_back() {
            Some(elem) => Ok(Some(elem)),
            _ => Ok(None),
        }
    }

    pub(crate) fn list_pop_multi_front(
        &mut self,
        key: &str,
        n: usize,
    ) -> Result<Option<Vec<String>>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        if array.is_empty() {
            return Ok(None);
        }

        let mut out = vec![];
        for _ in 0..n {
            if array.is_empty() {
                break;
            }

            out.push(array.pop_front().unwrap());
        }

        Ok(Some(out))
    }

    pub(crate) fn list_pop_multi_back(
        &mut self,
        key: &str,
        n: usize,
    ) -> Result<Option<Vec<String>>, String> {
        self.assert_array(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::Array(array) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        if array.is_empty() {
            return Ok(None);
        }

        let mut out = vec![];
        for _ in 0..n {
            if array.is_empty() {
                break;
            }

            out.push(array.pop_back().unwrap());
        }

        Ok(Some(out))
    }

    pub(crate) fn get_key_type_name(&self, key: &str) -> &str {
        self.dict
            .get(key)
            .map(|elem| elem.type_name())
            .unwrap_or("none")
    }

    pub(crate) fn stream_push(
        &mut self,
        key: String,
        id: StreamEntryID,
        kvpairs: Vec<KeyValuePair>,
    ) -> Result<CompleteStreamEntryID, String> {
        self.assert_stream(&key)?;

        let stream = self.dict.entry(key).or_insert(Entry::Stream(Vec::new()));
        let Entry::Stream(stream) = stream else {
            unreachable!()
        };

        let id = Self::resolve_stream_entry_id(id, stream)?;

        stream.push(StreamValue::new(id.clone(), kvpairs));

        Ok(id)
    }

    pub(crate) fn stream_get_range(
        &self,
        key: &str,
        start: &CompleteStreamEntryID,
        end: &CompleteStreamEntryID,
        count: usize,
    ) -> Result<Vec<StreamValue>, String> {
        self.stream_read_single_from_id_exclusive(key, start, true, end, true, count)
    }

    pub(crate) fn stream_read_multi_from_id_exclusive(
        &self,
        key_id_pairs: &Vec<(String, CompleteStreamEntryID)>,
        count: usize,
    ) -> Result<Vec<(String, Vec<StreamValue>)>, String> {
        let mut streams = vec![];

        for (key, start) in key_id_pairs {
            let stream = self.stream_read_single_from_id_exclusive(
                &key,
                start,
                false,
                &CompleteStreamEntryID::max(),
                true,
                count,
            )?;

            if !stream.is_empty() {
                streams.push((key.clone(), stream));
            }
        }

        Ok(streams)
    }

    pub(crate) fn resolve_latest_stream_id(
        &self,
        key: &str,
    ) -> Result<CompleteStreamEntryID, String> {
        self.assert_stream(&key)?;

        if !self.dict.contains_key(key) {
            return Ok(CompleteStreamEntryID(0, 0));
        }

        let Entry::Stream(stream) = self.dict.get(key).unwrap() else {
            unreachable!()
        };

        if stream.is_empty() {
            return Ok(CompleteStreamEntryID(0, 0));
        }

        Ok(stream.last().unwrap().id.clone())
    }

    pub(crate) fn incr(&mut self, key: &str) -> Result<i64, String> {
        self.assert_single_value(key)?;

        let Entry::Value(value_entry) =
            self.dict
                .entry(key.to_string())
                .or_insert(Entry::Value(ValueEntry {
                    value: vec![b'0'],
                    expiry_timestamp_ms: None,
                }))
        else {
            unreachable!()
        };

        let num = i64::from_str_radix(&value_entry.to_string()?, 10)
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?
            + 1;

        value_entry.set_from_str(&num.to_string());

        Ok(num)
    }

    pub(crate) fn keys(&self, raw_pattern: &str) -> Vec<String> {
        let mut out = vec![];
        let matcher = PatternMatcher::new(raw_pattern);

        for key in self.dict.keys() {
            if matcher.is_match(key) {
                out.push(key.clone());
            }
        }

        out
    }

    pub(crate) fn add_score_to_sorted_set(
        &mut self,
        key: &String,
        args: &Vec<(f64, String)>,
    ) -> Result<usize, String> {
        self.assert_set(key)?;

        let Entry::SortedSet(entry) = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::SortedSet(SortedSet::default()))
        else {
            unreachable!();
        };

        let mut new_items = 0;
        for (score, member) in args {
            if entry.insert_score(*score, member.clone()) {
                new_items += 1;
            }
        }

        Ok(new_items)
    }

    pub(crate) fn add_geo_to_sorted_set(
        &mut self,
        key: &String,
        args: &Vec<(f64, f64, String)>,
    ) -> Result<usize, String> {
        self.assert_set(key)?;

        let Entry::SortedSet(entry) = self
            .dict
            .entry(key.clone())
            .or_insert(Entry::SortedSet(SortedSet::default()))
        else {
            unreachable!();
        };

        let mut new_items = 0;
        for (lon, lat, member) in args {
            if lon < &MIN_LON || lon > &MAX_LON || lat < &MIN_LAT || lat > &MAX_LAT {
                return Err(format!(
                    "ERR invalid longitude,latitude pair {},{}",
                    lon, lat
                ));
            }

            if entry.insert_geo(*lon, *lat, member.clone()) {
                new_items += 1;
            }
        }

        Ok(new_items)
    }

    pub(crate) fn sorted_set_geopos(
        &self,
        key: &str,
        members: &Vec<String>,
    ) -> Result<Vec<Option<(f64, f64)>>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(members.iter().map(|_| None).collect());
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        let mut coords = vec![];

        for member in members {
            if let Some((lon, lat)) = set.member_coords(member) {
                coords.push(Some((lon, lat)));
                continue;
            }

            coords.push(None);
        }

        Ok(coords)
    }

    pub(crate) fn sorted_set_geodist(
        &self,
        key: &str,
        member_lhs: &str,
        member_rhs: &str,
    ) -> Result<Option<f64>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        if let Some(coord_lhs) = set.member_coords(member_lhs) {
            if let Some(coord_rhs) = set.member_coords(member_rhs) {
                let (lon1, lat1) = decode_geohash(encode_geohash(coord_lhs.0, coord_lhs.1));
                let (lon2, lat2) = decode_geohash(encode_geohash(coord_rhs.0, coord_rhs.1));
                return Ok(Some(geohash_get_distance(lon1, lat1, lon2, lat2)));
            }
        }

        Ok(None)
    }

    pub(crate) fn sorted_set_rank(&self, key: &str, member: &str) -> Result<Option<usize>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(set.rank(member))
    }

    pub(crate) fn sorted_set_range(
        &self,
        key: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<String>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(vec![]);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(set.range(
            resolve_start_index(start, set.len()),
            resolve_end_index(end, set.len()),
        ))
    }

    pub(crate) fn sorted_set_len(&self, key: &str) -> Result<usize, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(0);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(set.len())
    }

    pub(crate) fn sorted_set_member_score(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<f64>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(None);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        Ok(set.member_score(member))
    }

    pub(crate) fn sorted_set_remove_members(
        &mut self,
        key: &str,
        members: Vec<String>,
    ) -> Result<usize, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(0);
        }

        let Entry::SortedSet(set) = self.dict.get_mut(key).unwrap() else {
            unreachable!();
        };

        let mut total = 0;
        for member in members {
            if set.remove(member) {
                total += 1;
            }
        }

        Ok(total)
    }

    pub(crate) fn sorted_set_geo_search(
        &self,
        key: &str,
        lon: f64,
        lat: f64,
        radius: f64,
    ) -> Result<Vec<String>, String> {
        self.assert_set(key)?;

        if !self.dict.contains_key(key) {
            return Ok(vec![]);
        }

        let Entry::SortedSet(set) = self.dict.get(key).unwrap() else {
            unreachable!();
        };

        let mut in_range = vec![];
        for member in set.members() {
            let coord = set.member_coords(member).unwrap();
            let dist = geohash_get_distance(lon, lat, coord.0, coord.1);
            if dist <= radius {
                in_range.push(member.clone());
            }
        }

        Ok(in_range)
    }

    fn stream_read_single_from_id_exclusive(
        &self,
        key: &str,
        start: &CompleteStreamEntryID,
        start_inclusive: bool,
        end: &CompleteStreamEntryID,
        end_inclusive: bool,
        count: usize,
    ) -> Result<Vec<StreamValue>, String> {
        self.assert_stream(key)?;

        if !self.dict.contains_key(key) {
            return Ok(vec![]);
        }

        let Entry::Stream(stream) = self.dict.get(key).unwrap() else {
            unreachable!()
        };
        let mut out = vec![];

        for elem in stream {
            if out.len() >= count {
                break;
            }

            if ((start_inclusive && &elem.id >= start) || (!start_inclusive && &elem.id > start))
                && ((end_inclusive && &elem.id <= end) || (!end_inclusive && &elem.id < end))
            {
                out.push(elem.clone());
            }
        }

        Ok(out)
    }

    fn resolve_stream_entry_id(
        id: StreamEntryID,
        stream: &StreamEntry,
    ) -> Result<CompleteStreamEntryID, String> {
        let ms = match &id {
            StreamEntryID::Full(id) => id.0,
            StreamEntryID::MsOnly(ms) => *ms,
            StreamEntryID::Wildcard => current_time_ms(),
        };

        let seq = match id {
            StreamEntryID::Full(id) => id.1,
            StreamEntryID::MsOnly(_) | StreamEntryID::Wildcard => {
                let mut max_available_idx = if ms == 0 { 1 } else { 0 };
                for entry in stream {
                    if entry.id.0 == ms && entry.id.1 >= max_available_idx {
                        max_available_idx = entry.id.1 + 1;
                    }
                }
                max_available_idx
            }
        };

        if ms == 0 && seq == 0 {
            return Err("ERR The ID specified in XADD must be greater than 0-0".into());
        }

        for entry in stream {
            if entry.id.0 > ms {
                return Err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        .into(),
                );
            }
            if entry.id.0 == ms {
                if seq <= entry.id.1 {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                }
            }
        }

        Ok(CompleteStreamEntryID(ms, seq))
    }

    fn assert_array(&self, key: &str) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_array()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }

    fn assert_single_value(&self, key: &str) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_value()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }

    fn assert_stream(&self, key: &str) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_stream()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }

    fn assert_set(&self, key: &str) -> Result<(), String> {
        if self.dict.contains_key(key) {
            if !self.dict.get(key).map(|v| v.is_set()).unwrap() {
                return Err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        }

        Ok(())
    }
}
