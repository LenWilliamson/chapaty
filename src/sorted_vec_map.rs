use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    hash::{Hash, Hasher},
    ops::{Index, IndexMut, Range},
};

/// A map that maintains entries in sorted order, optimized for small collections.
///
/// `SortedVecMap` uses a sorted `SmallVec` internally, providing performance for
/// small datasets (typically < 100 elements).
/// For larger collections, consider using `BTreeMap` or `HashMap`.
///
/// # Type Parameters
///
/// - `N`: Inline capacity (default: 8). Number of entries stored on the stack before heap allocation.
///
/// # Performance Characteristics
///
/// - Insertion: O(n) - requires maintaining sorted order
/// - Lookup: O(n) - linear scan
/// - Iteration: O(n) - sequential
/// - Memory: Stack-allocated for <= `N` elements
///
/// # Ordering Guarantees
///
/// - Keys are always maintained in sorted order by their `Ord` implementation
/// - Iteration order is deterministic and corresponds to key sort order
/// - Duplicate keys are automatically deduplicated (last write wins)
///
/// # Examples
///
/// ```rust
/// # use chapaty::sorted_vec_map::SortedVecMap;
/// // Default capacity of 8
/// let mut map = SortedVecMap::new();
/// map.insert("zebra", 3);
/// map.insert("apple", 1);
/// map.insert("mango", 2);
///
/// // Iteration is in sorted key order
/// let keys: Vec<_> = map.keys().copied().collect();
/// assert_eq!(keys, vec!["apple", "mango", "zebra"]);
///
/// // Lookup operations
/// assert_eq!(map.get(&"apple"), Some(&1));
/// assert_eq!(map.remove(&"mango"), Some(2));
/// assert!(!map.contains_key(&"mango"));
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct SortedVecMap<K, V, const N: usize = 8> {
    inner: SmallVec<[(K, V); N]>,
}

// Core implementation
impl<K: Ord, V> SortedVecMap<K, V> {
    /// Creates an empty `SortedVecMap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let map: SortedVecMap<i32, String> = SortedVecMap::new();
    /// assert!(map.is_empty());
    /// ```
    #[inline]
    pub const fn new() -> Self {
        Self {
            inner: SmallVec::new_const(),
        }
    }

    /// Creates a `SortedVecMap` with a specified capacity.
    ///
    /// The map will be able to hold at least `capacity` elements without
    /// reallocating. If `capacity` is 0, the map will not allocate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let map: SortedVecMap<i32, String> = SortedVecMap::with_capacity(10);
    /// assert!(map.capacity() >= 10);
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: SmallVec::with_capacity(capacity),
        }
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the map contains no elements.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// assert!(map.is_empty());
    /// map.insert(1, "a");
    /// assert!(!map.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of elements the map can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Clears the map, removing all key-value pairs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// map.clear();
    /// assert!(map.is_empty());
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Returns `true` if the map contains the specified key.
    ///
    /// # Performance
    ///
    /// This uses a linear scan, which is faster than binary search for small
    /// collections (< ~100 elements) due to better cache locality and branch
    /// prediction. Consider using binary search for larger collections.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert!(map.contains_key(&1));
    /// assert!(!map.contains_key(&2));
    /// ```
    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.iter().any(|(k, _)| k == key)
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.get(&1), Some(&"a"));
    /// assert_eq!(map.get(&2), None);
    /// ```
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Returns a mutable reference to the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// if let Some(v) = map.get_mut(&1) {
    ///     *v = "b";
    /// }
    /// assert_eq!(map.get(&1), Some(&"b"));
    /// ```
    #[inline]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// Returns a reference to the key-value pair corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.get_key_value(&1), Some((&1, &"a")));
    /// ```
    #[inline]
    pub fn get_key_value(&self, key: &K) -> Option<(&K, &V)> {
        self.inner
            .iter()
            .find(|(k, _)| k == key)
            .map(|(k, v)| (k, v))
    }

    /// Inserts a key-value pair into the map, maintaining sorted order.
    ///
    /// If the map did not have this key present, `None` is returned.
    /// If the map did have this key present, the value is updated, and the old
    /// value is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// assert_eq!(map.insert(37, "a"), None);
    /// assert_eq!(map.insert(37, "b"), Some("a"));
    /// assert_eq!(map.get(&37), Some(&"b"));
    /// ```
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        for (i, (k, v)) in self.inner.iter_mut().enumerate() {
            match key.cmp(k) {
                Ordering::Less => {
                    self.inner.insert(i, (key, value));
                    return None;
                }
                Ordering::Equal => {
                    return Some(std::mem::replace(v, value));
                }
                Ordering::Greater => continue,
            }
        }
        // Key is greater than all existing keys
        self.inner.push((key, value));
        None
    }

    /// Removes a key from the map, returning the value if it was present.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.remove(&1), Some("a"));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner
            .iter()
            .position(|(k, _)| k == key)
            .map(|pos| self.inner.remove(pos).1)
    }

    /// Removes a key from the map, returning the stored key and value if present.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(map.remove_entry(&1), Some((1, "a")));
    /// assert_eq!(map.remove_entry(&1), None);
    /// ```
    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        self.inner
            .iter()
            .position(|(k, _)| k == key)
            .map(|pos| self.inner.remove(pos))
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    /// map.insert(2, "b");
    /// map.insert(3, "c");
    /// map.retain(|&k, _| k % 2 == 1);
    /// assert_eq!(map.len(), 2);
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.inner.retain_mut(|(k, v)| f(k, v));
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut letters = SortedVecMap::new();
    ///
    /// for ch in "a short treatise on fungi".chars() {
    ///     letters.entry(ch).and_modify(|counter| *counter += 1).or_insert(1);
    /// }
    ///
    /// assert_eq!(letters[&'s'], 2);
    /// assert_eq!(letters[&'t'], 3);
    /// assert_eq!(letters[&'u'], 1);
    /// assert_eq!(letters.get(&'y'), None);
    /// ```
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        if let Some(pos) = self.inner.iter().position(|(k, _)| *k == key) {
            Entry::Occupied(OccupiedEntry {
                map: self,
                position: pos,
            })
        } else {
            Entry::Vacant(VacantEntry { key, map: self })
        }
    }

    /// Merges another map into this one, consuming both maps and maintaining sorted order.
    ///
    /// All key-value pairs from `other` are moved into `self`. Duplicate keys
    /// are deduplicated (the value from `other` takes precedence).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map1 = SortedVecMap::new();
    /// map1.insert(1, "a");
    ///
    /// let mut map2 = SortedVecMap::new();
    /// map2.insert(2, "b");
    ///
    /// let merged = map1.merge(map2);
    /// assert_eq!(merged.len(), 2);
    /// ```
    pub fn merge(mut self, mut other: Self) -> Self
    where
        K: Eq,
    {
        self.inner.append(&mut other.inner);
        self.sort_and_dedup();
        self
    }

    /// Appends all elements from another map into this one.
    ///
    /// After appending, the map is re-sorted and deduplicated to maintain invariants.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map1 = SortedVecMap::new();
    /// map1.insert(1, "a");
    ///
    /// let mut map2 = SortedVecMap::new();
    /// map2.insert(2, "b");
    ///
    /// map1.append(&mut map2);
    /// assert_eq!(map1.len(), 2);
    /// assert!(map2.is_empty());
    /// ```
    pub fn append(&mut self, other: &mut Self)
    where
        K: Eq,
    {
        self.inner.append(&mut other.inner);
        self.sort_and_dedup();
    }

    /// Returns an iterator over the keys in sorted order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(2, "b");
    /// map.insert(1, "a");
    ///
    /// let keys: Vec<_> = map.keys().copied().collect();
    /// assert_eq!(keys, vec![1, 2]);
    /// ```
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.inner.iter().map(|(k, _)| k)
    }

    /// Returns an iterator over the values in key-sorted order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(2, "b");
    /// map.insert(1, "a");
    ///
    /// let values: Vec<_> = map.values().copied().collect();
    /// assert_eq!(values, vec!["a", "b"]);
    /// ```
    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.iter().map(|(_, v)| v)
    }

    /// Returns a mutable iterator over the values in key-sorted order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// for val in map.values_mut() {
    ///     *val *= 2;
    /// }
    ///
    /// assert_eq!(map.get(&1), Some(&2));
    /// ```
    #[inline]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.inner.iter_mut().map(|(_, v)| v)
    }

    /// Returns an iterator over the key-value pairs in key-sorted order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(2, "b");
    /// map.insert(1, "a");
    ///
    /// for (key, value) in map.iter() {
    ///     println!("{}: {}", key, value);
    /// }
    /// ```
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter().map(|(k, v)| (k, v))
    }

    /// Returns a mutable iterator over the key-value pairs in key-sorted order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map = SortedVecMap::new();
    /// map.insert(1, "a");
    ///
    /// for (_, value) in map.iter_mut() {
    ///     *value = "b";
    /// }
    /// ```
    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.inner.iter_mut().map(|(k, v)| (&*k, v))
    }

    /// Sorts the internal storage and removes duplicate keys.
    ///
    /// This is called automatically by methods that could break the sort invariant.
    /// You typically don't need to call this manually.
    fn sort_and_dedup(&mut self)
    where
        K: Eq,
    {
        self.inner.sort_by(|a, b| a.0.cmp(&b.0));
        self.inner.dedup_by(|a, b| a.0 == b.0);
    }
}

// Parallel iteration support (requires rayon)
impl<K: Ord + Sync, V: Sync> SortedVecMap<K, V> {
    /// Returns a parallel iterator over the key-value pairs.
    ///
    /// Requires the `rayon` feature to be enabled.
    #[inline]
    pub fn par_iter(&self) -> impl ParallelIterator<Item = (&K, &V)> {
        self.inner.par_iter().map(|(k, v)| (k, v))
    }
}

// ================================================================================================
// Entry API
// ================================================================================================

pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

pub struct OccupiedEntry<'a, K, V> {
    map: &'a mut SortedVecMap<K, V>,
    position: usize,
}

pub struct VacantEntry<'a, K, V> {
    key: K,
    map: &'a mut SortedVecMap<K, V>,
}

impl<'a, K: Ord, V> Entry<'a, K, V> {
    /// Ensures a value is in the entry by inserting the default if empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(3);
    /// assert_eq!(map[&"poneyland"], 3);
    /// ```
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the function if empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, String> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert_with(|| "hoho".to_string());
    /// ```
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default()),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the default
    /// function if empty. The key is passed to the function.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, String> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert_with_key(|key| key.to_uppercase());
    /// ```
    pub fn or_insert_with_key<F: FnOnce(&K) -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let value = default(&e.key);
                e.insert(value)
            }
        }
    }

    /// Provides in-place mutable access to an occupied entry before any
    /// potential inserts into the map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland")
    ///    .and_modify(|e| *e += 1)
    ///    .or_insert(42);
    /// assert_eq!(map[&"poneyland"], 42);
    /// ```
    pub fn and_modify<F: FnOnce(&mut V)>(mut self, f: F) -> Self {
        match &mut self {
            Entry::Occupied(e) => f(e.get_mut()),
            Entry::Vacant(_) => {}
        }
        self
    }

    /// Returns a reference to this entry's key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
    /// ```
    pub fn key(&self) -> &K {
        match self {
            Entry::Occupied(e) => e.key(),
            Entry::Vacant(e) => &e.key,
        }
    }
}

impl<'a, K: Ord, V: Default> Entry<'a, K, V> {
    /// Ensures a value is in the entry by inserting the default value if empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, Option<u32>> = SortedVecMap::new();
    /// map.entry("poneyland").or_default();
    /// assert_eq!(map[&"poneyland"], None);
    /// ```
    pub fn or_default(self) -> &'a mut V {
        self.or_insert_with(Default::default)
    }
}

impl<'a, K: Ord, V> VacantEntry<'a, K, V> {
    /// Gets a reference to the key that would be used when inserting a value
    /// through the `VacantEntry`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
    /// ```
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Take ownership of the key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    ///
    /// if let Entry::Vacant(v) = map.entry("poneyland") {
    ///     v.into_key();
    /// }
    /// ```
    pub fn into_key(self) -> K {
        self.key
    }

    /// Sets the value of the entry with the VacantEntry's key,
    /// and returns a mutable reference to it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    ///
    /// if let Entry::Vacant(o) = map.entry("poneyland") {
    ///     o.insert(37);
    /// }
    /// assert_eq!(map[&"poneyland"], 37);
    /// ```
    pub fn insert(self, value: V) -> &'a mut V {
        // Find insertion position while we still have self.key
        let pos = self
            .map
            .inner
            .iter()
            .position(|(k, _)| k > &self.key)
            .unwrap_or(self.map.inner.len());

        // Insert at the correct position
        self.map.inner.insert(pos, (self.key, value));

        // Return mutable reference to the value we just inserted
        &mut self.map.inner[pos].1
    }
}

impl<'a, K: Ord, V> OccupiedEntry<'a, K, V> {
    /// Gets a reference to the key in the entry.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::SortedVecMap;
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
    /// ```
    pub fn key(&self) -> &K {
        &self.map.inner[self.position].0
    }

    /// Gets a reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(o) = map.entry("poneyland") {
    ///     assert_eq!(o.get(), &12);
    /// }
    /// ```
    pub fn get(&self) -> &V {
        &self.map.inner[self.position].1
    }

    /// Gets a mutable reference to the value in the entry.
    ///
    /// If you need a reference to the `OccupiedEntry` which may outlive the
    /// destruction of the `Entry` value, see [`into_mut`].
    ///
    /// [`into_mut`]: OccupiedEntry::into_mut
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(mut o) = map.entry("poneyland") {
    ///     *o.get_mut() += 10;
    ///     assert_eq!(*o.get(), 22);
    /// }
    /// assert_eq!(map[&"poneyland"], 22);
    /// ```
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.map.inner[self.position].1
    }

    /// Converts the `OccupiedEntry` into a mutable reference to the value in the entry
    /// with a lifetime bound to the map itself.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(o) = map.entry("poneyland") {
    ///     *o.into_mut() += 10;
    /// }
    /// assert_eq!(map[&"poneyland"], 22);
    /// ```
    pub fn into_mut(self) -> &'a mut V {
        &mut self.map.inner[self.position].1
    }

    /// Sets the value of the entry, and returns the entry's old value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(mut o) = map.entry("poneyland") {
    ///     assert_eq!(o.insert(15), 12);
    /// }
    /// assert_eq!(map[&"poneyland"], 15);
    /// ```
    pub fn insert(&mut self, value: V) -> V {
        std::mem::replace(&mut self.map.inner[self.position].1, value)
    }

    /// Takes the value out of the entry, and returns it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(o) = map.entry("poneyland") {
    ///     assert_eq!(o.remove(), 12);
    /// }
    /// assert!(!map.contains_key(&"poneyland"));
    /// ```
    pub fn remove(self) -> V {
        self.map.inner.remove(self.position).1
    }

    /// Take the ownership of the key and value from the map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::sorted_vec_map::{SortedVecMap, Entry};
    /// let mut map: SortedVecMap<&str, u32> = SortedVecMap::new();
    /// map.entry("poneyland").or_insert(12);
    ///
    /// if let Entry::Occupied(o) = map.entry("poneyland") {
    ///     assert_eq!(o.remove_entry(), ("poneyland", 12));
    /// }
    /// assert!(!map.contains_key(&"poneyland"));
    /// ```
    pub fn remove_entry(self) -> (K, V) {
        self.map.inner.remove(self.position)
    }
}

// ================================================================================================
// Standard trait implementations
// ================================================================================================
impl<K: Ord, V> Extend<(K, V)> for SortedVecMap<K, V>
where
    K: Eq,
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        self.inner.extend(iter);
        self.sort_and_dedup();
    }
}

impl<K, V, const N: usize> IntoIterator for SortedVecMap<K, V, N> {
    type Item = (K, V);
    type IntoIter = smallvec::IntoIter<[(K, V); N]>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a SortedVecMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::iter::Map<std::slice::Iter<'a, (K, V)>, fn(&'a (K, V)) -> (&'a K, &'a V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter().map(|(k, v)| (k, v))
    }
}

impl<'a, K, V> IntoIterator for &'a mut SortedVecMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter =
        std::iter::Map<std::slice::IterMut<'a, (K, V)>, fn(&'a mut (K, V)) -> (&'a K, &'a mut V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut().map(|(k, v)| (&*k, v))
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for SortedVecMap<K, V>
where
    K: Eq,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut map = Self {
            inner: iter.into_iter().collect(),
        };
        map.sort_and_dedup();
        map
    }
}

impl<K: Ord, V> From<HashMap<K, V>> for SortedVecMap<K, V>
where
    K: Eq,
{
    fn from(hash_map: HashMap<K, V>) -> Self {
        let mut map = Self {
            inner: hash_map.into_iter().collect(),
        };
        map.sort_and_dedup();
        map
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for SortedVecMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<K: Eq, V: Eq> Eq for SortedVecMap<K, V> {}

impl<K: Ord, V> From<(K, V)> for SortedVecMap<K, V> {
    fn from(tuple: (K, V)) -> Self {
        let mut inner = SmallVec::new();
        inner.push(tuple);
        Self { inner }
    }
}

impl<K: Ord, V: PartialOrd> PartialOrd for SortedVecMap<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<K: Ord, V: Ord> Ord for SortedVecMap<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<K: Hash, V: Hash> Hash for SortedVecMap<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<K: Ord, V, const N: usize> From<[(K, V); N]> for SortedVecMap<K, V>
where
    K: Eq,
{
    fn from(arr: [(K, V); N]) -> Self {
        let mut map = Self {
            inner: SmallVec::from_iter(arr),
        };
        map.sort_and_dedup();
        map
    }
}

impl<K: Ord, V> From<Vec<(K, V)>> for SortedVecMap<K, V>
where
    K: Eq,
{
    fn from(vec: Vec<(K, V)>) -> Self {
        let mut map = Self {
            inner: SmallVec::from_vec(vec),
        };
        map.sort_and_dedup();
        map
    }
}

impl<K: Ord, V> Index<&K> for SortedVecMap<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        self.get(key).expect("key not found")
    }
}

impl<K: Ord, V> IndexMut<&K> for SortedVecMap<K, V> {
    fn index_mut(&mut self, key: &K) -> &mut Self::Output {
        self.get_mut(key).expect("key not found")
    }
}

impl<K, V> Index<Range<usize>> for SortedVecMap<K, V> {
    type Output = [(K, V)];
    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl<K, V> IndexMut<Range<usize>> for SortedVecMap<K, V> {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        &mut self.inner[index]
    }
}
