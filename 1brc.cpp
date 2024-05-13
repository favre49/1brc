#include <bits/stdc++.h>
#include <bits/extc++.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifndef NUM_THREADS
#define NUM_THREADS 8
#endif

struct Data {
  std::string name = "";
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
  __gnu_pbds::gp_hash_table<std::string, Data> temp_map;

  ThreadData() = default;
};

void process_chunk(const char* data, int start, int end, ThreadData& state) {
  // Go back until start points to a '\n'
  while(start >= 0 && data[start] != '\n') start--;
  start++;

  // Go back until end points to a '\n'
  while(end >= 0 && data[end] != '\n') end--;

  // Now [start, end] is the set of lines we want
  std::string name;
  name.reserve(100);
  int temp = 0, coeff = 1;
  bool name_mode = true;
  for (int i = start; i <= end; i++) {
    const char& c = data[i];
    if (c == '\n') {
      state.temp_map[name] += coeff*temp;

      // reset
      name_mode = true;
      temp =0;
      coeff = 1;
      name.clear();
    } else if (c == ';') {
      name_mode = false;
    } else if (c == '.') continue;
    else if (name_mode) {
      name.push_back(c);
    } else {
      if (c == '-') {
        coeff = -1;
      } else {
        temp = 10*temp + (c - '0');
      }
    }
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
  int file_size = (int)file_stat.st_size;

  const char* mapped_data = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
  if (mapped_data == MAP_FAILED) {
    std::cerr << "Failed to mmap file" << std::endl;
    return 1;
  }

  std::vector<std::thread> threads;
  threads.reserve(NUM_THREADS);
  std::vector<ThreadData> states(NUM_THREADS);
  int chunk_size = (file_size + NUM_THREADS - 1)/NUM_THREADS, last = 0;
  for (int i = 0; i < NUM_THREADS; i++) {
    int end = std::min(last + chunk_size, file_size);
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
  sort(data.begin(), data.end(), [&](const auto& x, const auto& y)->bool {
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
