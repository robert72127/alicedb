#ifndef ALICEDBDCOMMON
#define ALICEDBDCOMMON

#include <chrono>

namespace AliceDB {

#define PageSize 4096

using index = long long;
using timestamp = unsigned long long;

struct Delta {
	timestamp ts;
	int count;
};

template <typename Type>
struct Change {
	Delta delta;
	Type data;
};

// for automatically sorting by delta
struct DeltaComparator {
	bool operator()(const Delta &a, const Delta &b) const {
		return a.ts < b.ts; // Sort based on the timestamp
	}
};

inline timestamp get_current_timestamp() {
	auto now = std::chrono::system_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
	return duration.count();
}

struct TablePosition {
	uint32_t page_index;
	uint32_t tuple_index;
};

struct GarbageCollectSettings {
	// how often to run
	timestamp clean_freq_;
	// how old (current_ts - delete_age ) tuples should be deleted
	timestamp delete_age_;
	bool use_garbage_collector;
	bool remove_zeros_only;
};

} // namespace AliceDB

#endif