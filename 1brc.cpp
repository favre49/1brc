#include <bits/stdc++.h>
#include <bits/extc++.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <immintrin.h>

#ifndef NUM_THREADS
#define NUM_THREADS 8
#endif

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

using hash_t = uint32_t;

// StationName is a specialized string for storing station names.
// It enforces cache line byte alignment to optimize loads.
struct StationName {
  alignas(32) char s[32];
  size_t len;
  const char* ptr;

  StationName() = default;

  StationName(const char* data, size_t length): len(length) {
    if (likely(len <= 32)) {
      std::memset(s, 0, 32);
      std::memcpy(s, data, len);
    } else {
      ptr = data;
    }
  }

  // This won't be part of the hot loop, so optimizing it too much
  // isn't important.
  bool operator<(const StationName& o) const {
    const char *this_ptr = nullptr, *other_ptr = nullptr;
    int this_len = size(), other_len = o.size();
    if (likely(this_len <= 32)) this_ptr = s; 
    else this_ptr = ptr;
    if (likely(other_len <= 32)) other_ptr = o.s; 
    else other_ptr = o.ptr;
    // Compare the two strings
    int min_len = std::min(this_len, other_len);
    int res = std::memcmp(this_ptr, other_ptr, min_len);
    if (res < 0) return true;
    else if (res == 0) return this_len < other_len;
    return false;
  }

  friend std::ostream &operator<<(std::ostream &os, const StationName& s) {
    if (likely(s.len <= 32)) {
      return os << s.s;
    } 
    for (int i =0; i < s.len; i++) os << s.ptr[i];
    return os;
  }

  size_t size() const {
    return len;
  }
};

struct Data {
  StationName name;
  int count = 0;
  int64_t sum = 0;
  int min = std::numeric_limits<int>::max();
  int max = std::numeric_limits<int>::min();

  Data() = default;

  Data& operator+=(const Data& other) {
    count += other.count;
    sum += other.sum;
    min = std::min(other.min, min);
    max = std::max(other.max, max);
    return *this;
  } 

  Data& operator+=(int temp) {
    count++;
    sum += temp;
    min = std::min(min, temp);
    max = std::max(max, temp);
    return *this;
  }
};


// Open addressed hash map that uses a fixed array size and linear probing.
// Assumes that the number of entries is less than SIZE.
// Does not support deletion.
// K must be default constructible.
template<const int SIZE = 1<<15>
struct FixedHashMap {
  // Hash map entry. This appears to be faster than std::optional
  // and emplace().
  struct Entry {
    bool exists;
    std::pair<StationName, Data> value;
  };

  // Forward iterator for FixedHashMap.
  struct iterator {
    using value_type = std::pair<StationName, Data>;
    using difference_type = ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::forward_iterator_tag;

    std::array<Entry, SIZE> &data;
    size_t idx;

    iterator(std::array<Entry, SIZE>& __data, size_t __idx = 0)
      : data(__data), idx(__idx) {
        while(idx < SIZE && !data[idx].exists) idx++;
      }

    reference operator*() const {
      return data[idx].value;
    }

    pointer operator->() const {
      return &(data[idx].value);
    }

    iterator& operator++() {
      do {
        idx++;
      } while(idx < SIZE && !data[idx].exists);
      return *this;
    }

    iterator operator++(int) {
      iterator temp = *this;
      ++(*this);
      return temp;
    }

    bool operator==(const iterator& other) const {
      return std::addressof(data) == std::addressof(other.data) && idx == other.idx;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }
  };

  std::array<Entry, SIZE> hash_array;

  FixedHashMap() {
    // SIZE must be a power of 2 so that we can use masking.
    static_assert((SIZE&(SIZE-1)) == 0);
  }

  size_t inline __attribute__((always_inline)) array_index(const size_t x) {
    return x&(SIZE-1);
  }

  iterator begin() {
    return iterator(hash_array);
  }

  iterator end() {
    return iterator(hash_array, SIZE);
  }

  Data& probe_with_short_string(const char* data, int len, const __m256i vec, hash_t h) {
    auto key_hash = h;
    for (;;key_hash++) {
      size_t idx = array_index(key_hash);
      if (unlikely(!hash_array[idx].exists)) {
        hash_array[idx].exists = 1;
        hash_array[idx].value.first = StationName(data, len);
        return hash_array[idx].value.second;
      }
      if (likely(hash_array[idx].value.first.size() <= 32)) {
        // This has to be true since the string is short.
        auto window = _mm256_load_si256(reinterpret_cast<const __m256i*>(hash_array[idx].value.first.s));
        auto neq = _mm256_xor_si256(window, vec);
        if (likely(_mm256_testz_si256(neq, neq))) {
          // The two strings are equal
          return hash_array[idx].value.second;
        }
      }
    }
  }

  Data& probe_with_long_string(const char* data, int len, hash_t h) {
    auto key_hash = h;
    for (;; key_hash++) {
      size_t idx = array_index(key_hash);
      if (unlikely(!hash_array[idx].exists)) {
        hash_array[idx].exists = 1;
        hash_array[idx].value.first = StationName(data, len);
        return hash_array[idx].value.second;
      }
      if (unlikely(hash_array[idx].value.first.size() > 32)) {
        // This has to be true since the string is long.
        if (unlikely(hash_array[idx].value.first.size() != len)) continue;
        if (likely(std::memcmp(hash_array[idx].value.first.ptr, data, len) == 0)) return hash_array[idx].value.second;
      }
    }
  }
};


struct ThreadData {
  FixedHashMap<> temp_map;

  ThreadData() = default;
};

// It should be possible to make this much better, still a placeholder.
hash_t inline __attribute__((always_inline)) mm256_hash(const __m256i val) {
  __m128i l = _mm256_extracti128_si256(val, 0);
  __m128i h = _mm256_extracti128_si256(val, 1);
  l = _mm_add_epi32(l, h);
  l = _mm_hadd_epi32(l, l);
  return _mm_extract_epi32(l, 0) + _mm_extract_epi32(l, 1);
}

size_t inline __attribute__((always_inline)) swar_parse_temperature(const char* data, int& temp) {
  int64_t word;
  memcpy(&word, data, sizeof(int64_t));
  int decimalSepPos = __builtin_ctzll(~word & 0x10101000);
  int shift = 28 - decimalSepPos;
  int64_t sgnd = (~word << 59) >> 63;
  int64_t designMask = ~(sgnd & 0xFF);
  int64_t digits = ((word & designMask) << shift) & 0x0F000F0F00L;
  int64_t absValue = ((digits * 0x640a0001) >> 32) & 0x3FF;
  temp = (absValue^sgnd) - sgnd;
  return (decimalSepPos>>3) + 2;
}

void inline __attribute__((always_inline)) simd_process(const char* data, const size_t start, const size_t end, ThreadData& state) {
  size_t last = start;
  const __m256i semicolon_mask = _mm256_set1_epi8(';');
  const __m256i index_mask = _mm256_set_epi8(31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  for (size_t i = start; i <= end; i++) {
    int sz = 0;
    hash_t h;

    // Use intrinsics to find the semicolon fast.
    // We can do two at a time since instruction throughput is 0.5.
    // Somehow lddqu gives a speedup here over loadu, but consensus online is
    // that there isn't a difference. So why?
    auto window1 = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(data+i));
    auto window2 = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(data+i+32));
    auto semicolons1 = _mm256_cmpeq_epi8(window1, semicolon_mask);
    auto semicolons2 = _mm256_cmpeq_epi8(window2, semicolon_mask);
    int mask1 = _mm256_movemask_epi8(semicolons1);
    int mask2 = _mm256_movemask_epi8(semicolons2);
    // The most likely scnerio 
    if (likely(mask1)) {
      int pos = __builtin_ctz(mask1); // semicolon position found
      __m256i pos_mask = _mm256_set1_epi8(pos);
      __m256i prefix_mask = _mm256_cmpgt_epi8(pos_mask, index_mask);
      __m256i prefix_window = _mm256_and_si256(window1, prefix_mask);

      // Hash the 256 bit string.
      h = mm256_hash(prefix_window);
      i += pos;
      sz = pos;

      // Skip the ;
      i++;

      int temp;
      size_t offset = swar_parse_temperature(data+i, temp);
      i += offset;
      state.temp_map.probe_with_short_string(data+last, sz, prefix_window, h) += temp;;
    } else if (mask2){
      // Just use the first 256 bits for the hash.
      h = mm256_hash(window1);
      int pos = __builtin_ctz(mask1); // semicolon position found
      i += pos+32;
      sz = pos+32;

      // Skip the ;
      i++;
      int temp;
      size_t offset = swar_parse_temperature(data+i, temp);
      i += offset;
      state.temp_map.probe_with_long_string(data+last, sz, h) += temp;
    } else {
      // Just use the first 256 bits for the hash.
      h = mm256_hash(window1);

      // We can do two at a time since throughput is 0.5.
      auto window1 = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(data+i+64));
      auto window2 = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(data+i+98));
      auto semicolons1 = _mm256_cmpeq_epi8(window1, semicolon_mask);
      auto semicolons2 = _mm256_cmpeq_epi8(window2, semicolon_mask);
      int mask1 = _mm256_movemask_epi8(semicolons1);
      int mask2 = _mm256_movemask_epi8(semicolons2);
      if (likely(mask1)) {
        int pos = __builtin_ctz(mask1);
        i += 32+pos;
        sz = 32+pos;
      } else {
        int pos = __builtin_ctz(mask2);
        i += 64+pos;
        sz = 64+pos;
      }

      // Skip the ;
      i++;
      int temp;
      size_t offset = swar_parse_temperature(data+i, temp);
      i += offset;
      state.temp_map.probe_with_long_string(data+last, sz, h) += temp;
    }

    // reset
    last = i+1;
  }
}

void process_chunk(const char* data, size_t start, size_t end, ThreadData& state) {
  // Go back until start points to a '\n', or you reach the beginning.
  while(start > 0 && data[start] != '\n') start--;
  if (start != 0) start++;

  // Go back until end points to a '\n'.
  while(end >= 0 && data[end] != '\n') end--;

  const size_t THREAD_BLOCKS = 2;
  simd_process(data, start, end, state);
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Wrong number of arguments passed. The correct usage is " << argv[0] << " <filename>"  << std::endl;
    return 1;
  }

  int fd = open(argv[1], O_RDONLY);
  if (fd == -1) {
    std::cerr << "Failed to open file" << std::endl;
    return 1;
  }

  struct stat file_stat;
  if (fstat(fd, &file_stat) == -1) {
    std::cerr << "Failed to fstat file" << std::endl;
    return 1;
  }
  size_t file_size = (size_t)file_stat.st_size;

  // Using MAP_POPULATE reduces execution time to a third when running cold,
  // but is worse when running warm.
  const char* mapped_data = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
  if (mapped_data == MAP_FAILED) {
    std::cerr << "Failed to mmap file" << std::endl;
    return 1;
  }

  std::vector<std::thread> threads;
  threads.reserve(NUM_THREADS);
  std::vector<ThreadData> states(NUM_THREADS);
  size_t chunk_size = (file_size + NUM_THREADS - 1)/NUM_THREADS, last = 0;
  for (int i = 0; i < NUM_THREADS; i++) {
    size_t end = std::min(last + chunk_size, file_size);
    threads.emplace_back(process_chunk, mapped_data, last, end, std::ref(states[i]));
    last = end;
  }

  for (auto& t : threads) t.join();

  // Merge all data into states[0]
  const __m256i index_mask = _mm256_set_epi8(31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  for (int j = 1; j < NUM_THREADS; j++) {
    for (auto& [k, v] : states[j].temp_map) {
      if (likely(k.size() <= 32)) {
        auto window = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(k.s));
        __m256i pos_mask = _mm256_set1_epi8(k.size());
        __m256i prefix_mask = _mm256_cmpgt_epi8(pos_mask, index_mask);
        __m256i prefix_window = _mm256_and_si256(window, prefix_mask);
        hash_t h = mm256_hash(prefix_window);
        states[0].temp_map.probe_with_short_string(k.s, k.len, prefix_window, h) += v;
      } else {
        auto window = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(k.ptr));
        hash_t h = mm256_hash(window);
        states[0].temp_map.probe_with_long_string(k.ptr, k.len, h) += v;
      }
    }
  }

  std::vector<Data> data;
  for (auto& [name, d] : states[0].temp_map) {
    d.name = name;
    data.push_back(d);
  }
  sort(data.begin(), data.end(), [&](const Data& x, const Data& y)->bool {
    return x.name < y.name;
  });

  std::cout << std::fixed << std::setprecision(1);
  std::cout << "{";
  for (int i = 0; i < data.size(); i++) {
    std::cout << data[i].name << "=";
    std::cout << data[i].min/10.0 << "/" << data[i].sum/double(10*data[i].count) <<  "/" << data[i].max/10.0;
    if (i+1 != data.size()) std::cout << ", ";
  }
  std::cout << "}";
  close(fd);

  return 0;
}
