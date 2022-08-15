#include <algorithm>
#include <chrono>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

class TimeLogger {
 public:
  TimeLogger() : prev_timestamp_(std::chrono::steady_clock::now()) {}

  void LogTime(const std::string& log_text) {
    const auto now = std::chrono::steady_clock::now();
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - prev_timestamp_).count();

    std::cout << "Time spent for " << log_text << ": " << milliseconds / 1000
              << "s " << milliseconds % 1000 << "ms" << std::endl;

    prev_timestamp_ = now;
  }

 private:
  std::chrono::steady_clock::time_point prev_timestamp_;
};

struct StatsData {
  std::string word;
  uint64_t count;

  // define operator so that sort will return
  // descending order for counts and ascending for words
  bool operator<(const StatsData& other) const {
    return (count > other.count) ||
           ((count == other.count) && (word < other.word));
  }
};

template <typename Iterator>
void ParallelMergeSort(Iterator begin, Iterator end,
               unsigned int N = std::thread::hardware_concurrency() / 2) {
  auto len = std::distance(begin, end);
  if (len <= 1024 || N < 2) {
    std::sort(begin, end);
    return;
  }

  Iterator mid = std::next(begin, len / 2);
  auto sort_part_async = std::async(ParallelMergeSort<Iterator>, begin, mid, N - 2);
  ParallelMergeSort(mid, end, N - 2);
  sort_part_async.wait();
  std::inplace_merge(begin, mid, end);
}

std::unordered_map<std::string, uint64_t> ReadFileAndCountStats(
    const std::string& input_filename) {
  std::ifstream in_file(input_filename);
  if (!in_file.is_open()) {
    const auto error_msg =
        "can't open input file for reading, filename: " + input_filename;
    std::cerr << error_msg << std::endl;
    throw std::runtime_error(error_msg);
  }
  char ch;

  std::string current_word;
  std::ostringstream current_word_stream;
  std::unordered_map<std::string, uint64_t> words_to_stat_count;

  try {
    while (in_file.get(ch)) {
      if (std::isalpha(ch)) {
        current_word_stream << static_cast<char>(std::tolower(ch));
      } else {
        if (current_word_stream.str() != "") {
          words_to_stat_count[current_word_stream.str()] += 1;
        }
        current_word_stream.str("");
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "Error occurred while reading the file: " << e.what()
              << std::endl;
    in_file.close();
    throw e;
  }

  in_file.close();

  if (current_word_stream.str() != "") {
    words_to_stat_count[current_word_stream.str()] += 1;
  }

  return words_to_stat_count;
}

std::vector<StatsData> ConvertAndSortStats(
    std::unordered_map<std::string, uint64_t>&& words_to_stat_count) {
  std::vector<StatsData> stats;
  stats.reserve(words_to_stat_count.size());

  for (auto& word_count : words_to_stat_count) {
    stats.emplace_back(
        StatsData{std::move(word_count.first), std::move(word_count.second)});
  }

  ParallelMergeSort(stats.begin(), stats.end());

  return stats;
}

void WriteStatsToFile(const std::vector<StatsData>& stats,
                      const std::string& output_filename) {
  std::ofstream out_file(output_filename, std::ios_base::out);
  if (!out_file.is_open()) {
    const auto error_msg =
        "can't open output file for writing, filename: " + output_filename;
    std::cerr << error_msg << std::endl;
    throw std::runtime_error(error_msg);
  }

  try {
    for (const auto& stat : stats) {
      out_file << stat.word << " " << stat.count << "\n";
    }
  } catch (const std::exception& e) {
    std::cerr << "Error occurred while writing into the file: " << e.what()
              << std::endl;
    out_file.close();
    throw e;
  }

  out_file.close();
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "usage: ./freq <fromfile> <tofile>\n";
    return 0;
  }

  TimeLogger time_logger;

  std::unordered_map<std::string, uint64_t> words_to_stat_count;
  try {
    words_to_stat_count = ReadFileAndCountStats(argv[1]);
  } catch (const std::exception& e) {
    // logging is done in ReadFileAndCountStats
    return 0;
  }

  time_logger.LogTime("read and count stats");

  const auto stats = ConvertAndSortStats(std::move(words_to_stat_count));
  words_to_stat_count.clear();

  time_logger.LogTime("sort stats");

  try {
    WriteStatsToFile(stats, argv[2]);
  } catch (const std::exception& e) {
    // logging is done in WriteStatsToFile
    return 0;
  }

  time_logger.LogTime("write stats");

  return 0;
}
