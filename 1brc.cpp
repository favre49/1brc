#include <bits/stdc++.h>
#include <bits/extc++.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifndef NUM_THREADS
#define NUM_THREADS 8
#endif

struct hash_t {
  const static uint32_t BASE = 1e9+123;
  uint32_t val;

  hash_t(): val(0) {}

  hash_t& operator+=(char c) {
    val = BASE*val + c;
    return *this;
  }
};


// Open addressed hash map that uses a fixed array size and linear probing.
// Assumes that the number of entries is less than SIZE.
// Does not support deletion.
template<typename K, typename V, const int SIZE = 1<<16>
struct FixedHashMap {
  // Hash map entry. This appears to be faster than std::optional
  // and emplace().
  struct Entry {
    bool exists;
    std::pair<K, V> value;
  };

  // Forward iterator for FixedHashMap.
  struct iterator {
    using value_type = std::pair<K, V>;
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

  template<typename T>
  V& operator[](T&& key) {
    hash_t h;
    for (auto& c : key) h += c;
    return at_with_hash(std::forward<T>(key), h);
  }

  template<typename T>
  V& at_with_hash(T&& key, hash_t h) {
    auto key_hash = h.val;
    for (;;key_hash++) {
      size_t idx = array_index(key_hash);
      if (!hash_array[idx].exists) {
        // key does not exist in the hash map, so create it.
        hash_array[idx].exists = 1;
        hash_array[idx].value.first = key;
        return hash_array[idx].value.second;
      }
      if (hash_array[idx].value.first == key) {
        // key already exists in the hash map - just return the reference
        return hash_array[idx].value.second;
      }
    }
    // UB. This should never happen, since we assume the number of elements is
    // less than SIZE.
    assert(false);
  }
};

struct Data {
  std::string_view name;
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

struct ThreadData {
  FixedHashMap<std::string_view, Data> temp_map;

  ThreadData() = default;
};

void process_chunk(const char* data, size_t start, size_t end, ThreadData& state) {
  // Go back until start points to a '\n', or you reach the beginning.
  while(start > 0 && data[start] != '\n') start--;
  if (start != 0) start++;

  // Go back until end points to a '\n'.
  while(end >= 0 && data[end] != '\n') end--;

  // Now [start, end] is the set of lines we want.
  size_t last = start;
  for (size_t i = start; i <= end; i++) {
    hash_t h;
    int sz = 0;
    while(data[i] != ';') {
      h += data[i++];
      sz++;
    }

    // Skip the ;
    i++;

    int coeff = 1, temp = 0;
    if (data[i] == '-') {
      coeff = -1;
      i++;
    }
    if (data[i+1] == '.') {
      temp = (data[i]*10 + data[i+2] - (11*'0'))*coeff;
      i += 2;
    } else {
      temp = (data[i]*100 + data[i+1] * 10 +  data[i+3] - (111*'0'))*coeff;
      i += 3;
    }

    state.temp_map.at_with_hash(std::string_view(data + last, sz), h) += coeff*temp;

    // reset
    i++;
    last = i+1;
  }
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
  for (int j = 1; j < NUM_THREADS; j++) {
    for (auto& [k, v] : states[j].temp_map) {
      states[0].temp_map[k] += v;
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
// Next improvement - calculate and send in hash while parsing.
