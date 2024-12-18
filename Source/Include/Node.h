/** @todo 1) allow for attaching new nodes to input/outut 2) switch to
 * 
 * ok we allow for 1 but without allowing for dynamically resizing graph, instead it cannot change after start command;
 * 
 * 
 * 
 * persistent storage */
/**
 *	actually don't allow attaching new nodes during runtime, nodes have to
 *be predefined at the start so single source might support multiple sinks, but
 *don't allow for modiication during runtime
 *
 *
 *
 *
 * ok let's think about how to implement this 1 problem)
 * first we actaully need to store vector of out_nodes and not single out_node,
 *it's just that for all other nodes than sink source, this vector will always
 *have 1 element
 *
 * When all out_queues are synchronized problem becomes very easy in fact,
 *operation look like now but instead of inserting to one out_queue we insert
 *into all out_queues,
 *
 * but before that we have one queue that is online and other that first need to
 *process all stale data ok let's add extra method to all
 *
 *
 *2)
 * for nodes that needs match fields store in rocks db <Key: match_fields| data
 *><Value Index> for normal ones <Key: data ><Value: Index>
 *
 * we also store <index, ts> <count> in separate storage to calculate deltas
 */

#ifndef ALICEDBNODE
#define ALICEDBNODE

#include "Common.h"
#include "Producer.h"
#include "Queue.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <type_traits>

template <typename Type> std::array<char, sizeof(Type)> Key(const Type &type) {
  std::array<char, sizeof(Type)> key;
  std::memcpy(key.data(), &type, sizeof(Type));
  return key;
}
template <typename Type> struct KeyHash {
  std::size_t operator()(const std::array<char, sizeof(Type)> &key) const {
    // You can use any suitable hash algorithm. Here, we'll use std::hash with
    // std::string_view
    return std::hash<std::string_view>()(
        std::string_view(key.data(), key.size()));
  }
};

#define DEFAULT_QUEUE_SIZE (200)

namespace AliceDB {

// generic compact deltas work's for almost any kind of node (doesn't work for
// aggregations)
void compact_deltas(
    std::vector<std::multiset<Delta, DeltaComparator>> &index_to_deltas,
    timestamp current_ts) {
  for (int index = 0; index < index_to_deltas.size(); index++) {
    auto &deltas = index_to_deltas[index];
    int previous_count = 0;
    timestamp ts = 0;

    for (auto it = deltas.rbegin(); it != deltas.rend();) {
      previous_count += it->count;
      ts = it->ts;
      auto base_it = std::next(it).base();
      base_it = deltas.erase(base_it);
      it = std::reverse_iterator<decltype(base_it)>(base_it);

      // Check the condition: ts < ts_ - frontier_ts_

      // if current delta has bigger tiemstamp than one we are setting, or we
      // iterated all deltas insert accumulated delta and break loop
      if (it == deltas.rend() || it->ts > current_ts) {
        deltas.insert(Delta{ts, previous_count});
        break;
      } else {
        continue;
      }
    }
  }
}

class Node {

public:
  virtual ~Node(){};

  virtual void Compute() = 0;

  /**
   * @brief update lowest ts this node will need to hold
   */
  virtual void UpdateTimestamp(timestamp ts) = 0;

  virtual timestamp GetFrontierTs() const = 0;

  /**
   * @brief returns Queue corresponding to output from this Tuple
   */
  virtual Queue *Output() = 0;

  /** returns vector of all inputs to this node */
  virtual std::vector<Node *> Inputs() = 0;
};

/* Source node is responsible for producing data through Compute function and
 * then writing output to both out_queue, and persistent table creator of this
 * node needs to specify how long delayed data might arrive
 */
template <typename Type> class SourceNode : public Node {
public:
  SourceNode(Producer<Type> *prod, timestamp frontier_ts, int duration_us = 500)
      : produce_{prod}, frontier_ts_{frontier_ts}, duration_us_{duration_us} {
    this->produce_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
    this->ts_ = get_current_timestamp();
  }

  void Compute() {
    auto start = std::chrono::steady_clock::now();
    std::chrono::microseconds duration(this->duration_us_);
    auto end = start + duration;
	int cnt = 0;
    char *prod_data;
    // produce some data with time limit set, into produce_queue
    while (std::chrono::steady_clock::now() < end) {
      produce_queue_->ReserveNext(&prod_data);
      Tuple<Type> *prod_tuple = (Tuple<Type> *)(prod_data);
      prod_tuple->delta.ts = get_current_timestamp();
      if (!this->produce_->next(prod_tuple)) {
        // we reserved but won't insert so have to remove it from queue
        produce_queue_->RemoveLast();
        break;
      }
	  cnt++;
    }

    // insert data from produce_queue into the table
    const char *in_data;
    while (this->produce_queue_->GetNext(&in_data)) {
      Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_.contains(Key<Type>(in_tuple->data))) {
        index match_index = this->next_index_;
        this->next_index_++;
        this->tuple_to_index_[Key<Type>(in_tuple->data)] = match_index;
        this->index_to_deltas_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_tuple->delta});
      } else {
        index match_index = this->tuple_to_index_[Key<Type>(in_tuple->data)];
        this->index_to_deltas_[match_index].insert(in_tuple->delta);
      }
    }

    if (this->update_ts_) {
      this->Compact();
      this->update_ts_ = false;
    }
  }

  Queue *Output() { return this->produce_queue_; }

  // source has no inputs
  std::vector<Node *> Inputs() { return {}; }

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->ts_ = ts;
      this->update_ts_ = true;
    }
    return;
  }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

private:
  void Compact() { compact_deltas(this->index_to_deltas_, this->ts_); }

  int duration_us_;

  size_t next_index_ = 0;

  bool update_ts_ = false;

  // how much time back from current time do we have to store values
  timestamp frontier_ts_;

  // what is oldest timestamp that needs to be keept by this table
  timestamp ts_;

  Producer<Type> *produce_;

  // data from producer is put into this queue from it it's written into both
  // table and passed to output nodes
  Queue *produce_queue_;

  // we can treat whole tuple as a key, this will return it's index
  // we will later use some persistent storage for that mapping, maybe rocksdb
  // or something
  std::unordered_map<std::array<char, sizeof(Type)>, index, KeyHash<Type>>
      tuple_to_index_;
  // from index we can get list of changes to the input, later
  // we will use some better data structure for that
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_;
};

template <typename Type> class SinkNode : public Node {
public:
  SinkNode(Node *in_node)
      : in_node_(in_node), in_queue_{in_node->Output()},
        frontier_ts_{in_node->GetFrontierTs()} {
    this->ts_ = get_current_timestamp();}

  void Compute() {
    // write in queue into out_table
    const char *in_data;
    while (this->in_queue_->GetNext(&in_data)) {
      Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_.contains(Key<Type>(in_tuple->data))) {
        index match_index = this->next_index_;
        this->next_index_++;
        this->tuple_to_index_[Key<Type>(in_tuple->data)] = match_index;
        this->index_to_deltas_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_tuple->delta});
      } else {
        index match_index = this->tuple_to_index_[Key<Type>(in_tuple->data)];
        this->index_to_deltas_[match_index].insert(in_tuple->delta);
      }
    }

    // get current timestamp that can be considered
    this->UpdateTimestamp(get_current_timestamp());

    if (this->compact_) {
      this->Compact();
      this->update_ts_ = false;
    }
    this->in_queue_->Clean();
  }

  // print state of table at this moment
  void Print(timestamp ts, std::function<void(const char *)> print) {
    for (auto &pair : this->tuple_to_index_) {
      auto &current_data = pair.first;
      // iterate deltas from oldest till current
      int total = 0;
      index current_index = pair.second;

      std::multiset<Delta, DeltaComparator> &deltas =
          this->index_to_deltas_[current_index];
      for (auto dit = deltas.begin(); dit != deltas.end(); dit++) {
        const Delta &delta = *dit;
        if (delta.ts > ts) {
          break;
        } else {
          total += delta.count;
        }
      }
      // now print positive's
      std::cout << "COUNT : " << total << " |\t ";
      print(current_data.data());
    }
  }

  Queue *Output() { return nullptr; }

  // source has no inputs
  std::vector<Node *> Inputs() { return {this->in_node_}; }

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->update_ts_ = true;
      this->ts_ = ts;
      this->in_node_->UpdateTimestamp(ts);
    }
  }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

private:
  void Compact() { compact_deltas(this->index_to_deltas_, this->ts_); }

  size_t next_index_ = 0;
  bool compact_;

  bool update_ts_ = false;

  // how much time back from current time do we have to store values
  timestamp frontier_ts_;

  // what is oldest timestamp that needs to be keept by this table
  timestamp ts_;

  Node *in_node_;
  Queue *in_queue_;

  // we can treat whole tuple as a key, this will return it's index
  // we will later use some persistent storage for that mapping, maybe rocksdb
  // or something

  std::unordered_map<std::array<char, sizeof(Type)>, index, KeyHash<Type>>
      tuple_to_index_;
  // from index we can get list of changes to the input, later
  // we will use some better data structure for that
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_;
};

template <typename Type> class FilterNode : public Node {
public:
  FilterNode(Node *in_node, std::function<bool(const Type &)> condition)
      : condition_{condition}, in_node_{in_node}, in_queue_{in_node->Output()},
        frontier_ts_{in_node->GetFrontierTs()} {
    this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
    this->ts_ = get_current_timestamp();
  }

  // filters node
  // those that pass are put into output queue, this is all that this node does
  void Compute() {
    const char *data;
    // pass function that match condition to output
    while (this->in_queue_->GetNext(&data)) {
      const Tuple<Type> *tuple = (const Tuple<Type> *)(data);
      if (this->condition_(tuple->data)) {
        this->out_queue_->Insert(data);
      }
    }
    this->in_queue_->Clean();
  }

  Queue *Output() { return this->out_queue_; }

  std::vector<Node *> Inputs() { return {this->in_node_}; }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

  // timestamp is not used here but will be used for global state for
  // propagation
  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->ts_ = ts;
      this->in_node_->UpdateTimestamp(ts);
    }
  }

private:
  std::function<bool(const Type &)> condition_;

  // acquired from in node, this will be passed to output node
  timestamp frontier_ts_;

  // track this for global timestamp state update
  Node *in_node_;
  timestamp ts_;

  Queue *in_queue_;
  Queue *out_queue_;
};
// projection can be represented by single node
template <typename InType, typename OutType>
class ProjectionNode : public Node {
public:
  ProjectionNode(Node *in_node,
                 std::function<OutType(const InType &)> projection)
      : projection_{projection}, in_node_{in_node},
        in_queue_{in_node->Output()}, frontier_ts_{in_node->GetFrontierTs()} {
    this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<OutType>));
    this->ts_ = get_current_timestamp();
  }

  void Compute() {
    const char *in_data;
    char *out_data;
    while (in_queue_->GetNext(&in_data)) {
      this->out_queue_->ReserveNext(&out_data);
      Tuple<InType> *in_tuple = (Tuple<InType> *)(in_data);
      Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);

      out_tuple->delta.ts = in_tuple->delta.ts;
      out_tuple->delta.count = in_tuple->delta.count;
      out_tuple->data =  this->projection_(in_tuple->data);
    }

    this->in_queue_->Clean();
  }

  Queue *Output() { return this->out_queue_; }

  std::vector<Node *> Inputs() { return {this->in_node_}; }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

  // timestamp is not used here but might be helpful to store for propagatio
  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->ts_ = ts;
      this->in_node_->UpdateTimestamp(ts);
    }
  }

private:
  std::function<OutType(const InType &)> projection_;

  // track this for global timestamp state update
  Node *in_node_;
  timestamp ts_;

  // acquired from in node, this will be passed to output node
  timestamp frontier_ts_;

  Queue *in_queue_;
  Queue *out_queue_;
};

// we need distinct node that will:
// for tuples that have positive count -> produce tuple with count 1 if positive
// for tuples that have negative count -> produce tuple with count 0 otherwise

// but the catch is: what this node will emit depends fully on previouse state:
/*
        if previous state was 0
                if now positive emit 1
                if now negative don't emit
        if previouse state was 1
                if now positive don't emit
                if now negative emit -1

*/
template <typename Type> class DistinctNode : public Node {
public:
  DistinctNode(Node *in_node)
      : in_queue_{in_node->Output()}, in_node_{in_node},
        frontier_ts_{in_node->GetFrontierTs()} {
    this->ts_ = get_current_timestamp();
    this->previous_ts_ = 0;
    this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
  }

  Queue *Output() { return this->out_queue_; }

  std::vector<Node *> Inputs() { return {this->in_node_}; }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

  void Compute() {

    // first insert all new data from queue to table
    const char *in_data_;
    while (this->in_queue_->GetNext(&in_data_)) {
      const Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data_);
      // if this data wasn't present insert with new index
      
	  if (!this->tuple_to_index_.contains(Key<Type>(in_tuple->data))) {
        index match_index = this->next_index_;
        this->next_index_++;
        this->tuple_to_index_[Key<Type>(in_tuple->data)] = match_index;
        this->index_to_deltas_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_tuple->delta});
		emited_.push_back(false);
      } else {
        index match_index = this->tuple_to_index_[Key<Type>(in_tuple->data)];
        this->index_to_deltas_[match_index].insert(in_tuple->delta);
      }
    }

    // then if compact_
    if (this->compact_) {
      // delta_count for oldest keept verson fro this we can deduce what tuples
      // to emit;
      std::vector<Delta> oldest_deltas_{this->tuple_to_index_.size()};

      // emit delete for oldest keept version, emit insert for previous_ts

      // iterate by tuple to index
      for (auto it = this->tuple_to_index_.begin();
           it != this->tuple_to_index_.end(); it++) {
        oldest_deltas_[it->second] =
            *(this->index_to_deltas_[it->second].rbegin());
      }

      this->Compact();
      this->compact_ = false;

      for (auto it = this->tuple_to_index_.begin();
           it != this->tuple_to_index_.end(); it++) {
        // now we can get current oldest delta ie after compaction:
        Delta cur_delta = *(this->index_to_deltas_[it->second].rbegin());
        bool previous_positive = oldest_deltas_[it->second].count > 0;
        bool current_positive = cur_delta.count > 0;
        /*
                Now we can deduce what to emit based on this index value from
           oldest_delta, negative delta -> previouse state was 0 positive delta
           -> previouse state was 1

                if previous state was 0
                        if now positive emit 1
                        if now negative don't emit
                if previouse state was 1
                        if now positive don't emit
                        if now negative emit -1

        */
        char *out_data;

		// but if it's first iteration of this Node we need to always emit
		if (!this->emited_[it->second]){
			if (current_positive){
					this->out_queue_->ReserveNext(&out_data);
					Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
					update_tpl->delta.ts = cur_delta.ts;
					update_tpl->delta.count = 1;
					// finally copy data
					std::memcpy(&update_tpl->data, &it->first, sizeof(Type));
				
				this->emited_[it->second] = true;
			}
			
		}
		else {
			if (previous_positive) {
				if (current_positive) {
					continue;
				} else {
					this->out_queue_->ReserveNext(&out_data);
					Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
					update_tpl->delta.ts = cur_delta.ts;
					update_tpl->delta.count = -1;
					// finally copy data
					std::memcpy(&update_tpl->data, &it->first, sizeof(Type));
				}
			} 
			else {
				if (current_positive) {
					this->out_queue_->ReserveNext(&out_data);
					Tuple<Type> *update_tpl = (Tuple<Type> *)(out_data);
					update_tpl->delta.ts = cur_delta.ts;
					update_tpl->delta.count = 1;
					// finally copy data
					std::memcpy(&update_tpl->data, &it->first, sizeof(Type));
				} else {
					continue;
				}
			}
		}  
      }
    }
  }

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->compact_ = true;
      this->previous_ts_ = this->ts_;
      this->ts_ = ts;
      this->in_node_->UpdateTimestamp(ts);
    }
  }

private:
  void Compact() {
    // leave previous_ts version as oldest one
    compact_deltas(this->index_to_deltas_, this->previous_ts_);
  }

  // timestamp will be used to track valid tuples
  // after update propagate it to input nodes
  bool compact_ = false;
  timestamp ts_;

  timestamp previous_ts_ = 0;

  timestamp frontier_ts_;

  Node *in_node_;

  Queue *in_queue_;

  Queue *out_queue_;

  size_t next_index_ = 0;

  // we can treat whole tuple as a key, this will return it's index
  // we will later use some persistent storage for that mapping, maybe rocksdb
  // or something
  std::unordered_map<std::array<char, sizeof(Type)>, index, KeyHash<Type>>
      tuple_to_index_;
  // from index we can get multiset of changes to the input, later
  // we will use some better data structure for that, but for now multiset is
  // nice cause it auto sorts for us
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_;

  //whether tuple for given index was ever emited, this is needed for compute state machine
  std::vector<bool> emited_;

};

/**
 * @brief this will implement all but Compute functions for Stateful binary
 * nodes
 */

// stateless binary operator, combines data from both in queues and writes them
// to out_queue
/*
        Union is PlusNode -> DistinctNode
        Except is plus with NegateLeft -> DistinctNode
*/
template <typename Type> class PlusNode : public Node {
public:
  // we can make it subtractor by setting neg left to true, then left deltas are
  // reversed
  PlusNode(Node *in_node_left, Node *in_node_right, bool negate_left)
      : negate_left_(negate_left), in_node_left_{in_node_left},
        in_node_right_{in_node_right}, in_queue_left_{in_node_left->Output()},
        in_queue_right_{in_node_right->Output()},
        frontier_ts_{std::max(in_node_left->GetFrontierTs(),
                              in_node_right->GetFrontierTs())} {
    this->ts_ = get_current_timestamp();
    this->out_queue_ = new Queue(DEFAULT_QUEUE_SIZE, sizeof(Tuple<Type>));
  }

  Queue *Output() { return this->out_queue_; }

  std::vector<Node *> Inputs() {
    return {this->in_node_left_, this->in_node_right_};
  }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }

  void Compute() {

    // process left input
    const char *in_data;
    char *out_data;
    while (in_queue_left_->GetNext(&in_data)) {
      this->out_queue_->ReserveNext(&out_data);
      Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
      Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);

      out_tuple->delta.ts = in_tuple->delta.ts;
      out_tuple->delta.count = in_tuple->delta.count;
      std::memcpy(&out_tuple->data, &in_tuple->data, sizeof(Type));
    }
    in_data = nullptr;
    out_data = nullptr;
    this->in_queue_left_->Clean();

    // process right input
    while (in_queue_right_->GetNext(&in_data)) {
      this->out_queue_->ReserveNext(&out_data);
      Tuple<Type> *in_tuple = (Tuple<Type> *)(in_data);
      Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
      out_tuple->delta.ts = in_tuple->delta.ts;
      out_tuple->delta.count =
          (this->negate_left_) ? -in_tuple->delta.count : in_tuple->delta.count;
      std::memcpy(&out_tuple->data, &in_tuple->data, sizeof(Type));
    }

    this->in_queue_right_->Clean();
  
  }

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->ts_ = ts;
      this->in_node_left_->UpdateTimestamp(ts);
      this->in_node_right_->UpdateTimestamp(ts);
    }
  }

private:
  timestamp ts_;

  timestamp frontier_ts_;

  Node *in_node_left_;
  Node *in_node_right_;

  Queue *in_queue_left_;
  Queue *in_queue_right_;

  Queue *out_queue_;

  bool negate_left_;
};

template <typename LeftType, typename RightType, typename OutType>
class StatefulBinaryNode : public Node {
public:
  StatefulBinaryNode(Node *in_node_left, Node *in_node_right)
      : in_node_left_{in_node_left}, in_node_right_{in_node_right},
        in_queue_left_{in_node_left->Output()},
        in_queue_right_{in_node_right->Output()},
        frontier_ts_{std::max(in_node_left->GetFrontierTs(),
                              in_node_right->GetFrontierTs())} {
    this->ts_ = get_current_timestamp();
    this->out_queue_ =
        new Queue(DEFAULT_QUEUE_SIZE * 2, sizeof(Tuple<OutType>));
  }

  Queue *Output() { return this->out_queue_; }

  std::vector<Node *> Inputs() {
    return {this->in_node_left_, this->in_node_right_};
  }

  timestamp GetFrontierTs() const { return this->frontier_ts_; }
  virtual void Compute() = 0;

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->compact_ = true;
      this->ts_ = ts;
      this->in_node_left_->UpdateTimestamp(ts);
      this->in_node_right_->UpdateTimestamp(ts);
    }
  }

protected:
  void Compact() {
    compact_deltas(this->index_to_deltas_left_, this->ts_);
    compact_deltas(this->index_to_deltas_right_, this->ts_);
  }

  // timestamp will be used to track valid tuples
  // after update propagate it to input nodes
  bool compact_ = false;
  timestamp ts_;

  timestamp frontier_ts_;

  Node *in_node_left_;
  Node *in_node_right_;

  Queue *in_queue_left_;
  Queue *in_queue_right_;

  Queue *out_queue_;

  size_t next_index_left_ = 0;
  size_t next_index_right_ = 0;

  // we can treat whole tuple as a key, this will return it's index
  // we will later use some persistent storage for that mapping, maybe rocksdb
  // or something
  std::unordered_map<std::array<char, sizeof(LeftType)>, index,
                     KeyHash<LeftType>>
      tuple_to_index_left;
  std::unordered_map<std::array<char, sizeof(RightType)>, index,
                     KeyHash<RightType>>
      tuple_to_index_right;
  // from index we can get multiset of changes to the input, later
  // we will use some better data structure for that, but for now multiset is
  // nice cause it auto sorts for us
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_left_;
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_right_;

  std::mutex node_mutex;
};

// this is node behind Intersect
/** @todo it needs better name :) */
template <typename Type>
class IntersectNode : public StatefulBinaryNode<Type, Type, Type> {
public:
  IntersectNode(Node *in_node_left, Node *in_node_right)
      : StatefulBinaryNode<Type, Type, Type>(in_node_left, in_node_right) {}

  void Compute() {
    // compute right_queue against left_queue
    // they are small and hold no indexes so we do it just by nested loop
    const char *in_data_left;
    const char *in_data_right;
    char *out_data;
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
      while (this->in_queue_right_->GetNext(&in_data_right)) {
        Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
        // if left and right queues match on data put it into out_queue with new
        // delta
        if (!std::memcmp(&in_left_tuple->data, &in_right_tuple->data,
                         sizeof(Type))) {
          this->out_queue_->ReserveNext(&out_data);
          Delta out_delta = this->delta_function_(in_left_tuple->delta,
                                                  in_right_tuple->delta);
          Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
          std::memcpy(&out_tuple->data, &in_left_tuple->data, sizeof(Type));
          out_tuple->delta = out_delta;
        }
      }
    }

    // compute left queue against right table
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
      // get all matching on data from right
      if (this->tuple_to_index_right.contains(Key<Type>(in_left_tuple->data))) {
        index match_index =
            this->tuple_to_index_right[Key<Type>(in_left_tuple->data)];

        for (auto it = this->index_to_deltas_right_[match_index].begin();
             it != this->index_to_deltas_right_[match_index].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Delta out_delta = this->delta_function_(in_left_tuple->delta, *it);
          Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
          std::memcpy(&out_tuple->data, &in_left_tuple->data, sizeof(Type));
          out_tuple->delta = out_delta;
        }
      }
    }

    // compute right queue against left table
    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
      // get all matching on data from right
      if (this->tuple_to_index_left.contains(Key<Type>(in_right_tuple->data))) {
        index match_index =
            this->tuple_to_index_left[Key<Type>(in_right_tuple->data)];
        for (auto it = this->index_to_deltas_left_[match_index].begin();
             it != this->index_to_deltas_left_[match_index].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Delta out_delta = this->delta_function_(*it, in_right_tuple->delta);
          Tuple<Type> *out_tuple = (Tuple<Type> *)(out_data);
          std::memcpy(&out_tuple->data, &in_right_tuple->data, sizeof(Type));
          out_tuple->delta = out_delta;
        }
      }
    }

    // insert new deltas from in_queues
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<Type> *in_left_tuple = (Tuple<Type> *)(in_data_left);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_left.contains(Key<Type>(in_left_tuple->data))) {
        index match_index = this->next_index_left_;
        this->next_index_left_++;
        this->tuple_to_index_left[Key<Type>(in_left_tuple->data)] = match_index;
        this->index_to_deltas_left_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_left_tuple->delta});
      } else {
        index match_index =
            this->tuple_to_index_left[Key<Type>(in_left_tuple->data)];
        this->index_to_deltas_left_[match_index].insert(in_left_tuple->delta);
      }
    }
    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<Type> *in_right_tuple = (Tuple<Type> *)(in_data_right);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_right.contains(
              Key<Type>(in_right_tuple->data))) {
        index match_index = this->next_index_right_;
        this->next_index_right_++;
        this->tuple_to_index_right[Key<Type>(in_right_tuple->data)] =
            match_index;
        this->index_to_deltas_right_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_right_tuple->delta});
      } else {
        index match_index =
            this->tuple_to_index_left[Key<Type>(in_right_tuple->data)];
        this->index_to_deltas_right_[match_index].insert(in_right_tuple->delta);
      }
    }

    // clean in_queues
    this->in_queue_left_->Clean();
    this->in_queue_right_->Clean();

    if (this->compact_) {
      this->Compact();
    }
  }

private:
  // extra state beyond StatefulNode is only deltafunction
  static Delta delta_function_(const Delta &left_delta,
                               const Delta &right_delta) {
    return {std::max(left_delta.ts, right_delta.ts),
            left_delta.count * right_delta.count};
  }
};

template <typename InTypeLeft, typename InTypeRight, typename OutType>
class CrossJoinNode
    : public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
  CrossJoinNode(
      Node *in_node_left, Node *in_node_right,
  	std::function<OutType(const InTypeLeft&, const InTypeRight&)> join_layout)  
	  : StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left,
                                                             in_node_right),
        join_layout_{join_layout} {}

  void Compute() {

    // compute right_queue against left_queue
    // they are small and hold no indexes so we do it just by nested loop
    const char *in_data_left;
    const char *in_data_right;
    char *out_data;
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      while (this->in_queue_right_->GetNext(&in_data_right)) {
        Tuple<InTypeRight> *in_right_tuple =
            (Tuple<InTypeRight> *)(in_data_right);
        // if left and right queues match on data put it into out_queue with new
        // delta
        this->out_queue_->ReserveNext(&out_data);
        Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
        out_tuple->delta = {
            std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),
            in_left_tuple->delta.count * in_right_tuple->delta.count};
        out_tuple->data =  this->join_layout_(in_left_tuple->data, in_right_tuple->data);
      }
    }


    // compute left queue against right table
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      // get all matching on data from right
      for (auto &[table_data, table_idx] : this->tuple_to_index_right) {
        for (auto it = this->index_to_deltas_right_[table_idx].begin();
             it != this->index_to_deltas_right_[table_idx].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
          out_tuple->delta = {std::max(in_left_tuple->delta.ts, it->ts),
                              in_left_tuple->delta.count * it->count};
          out_tuple->data =  this->join_layout_(in_left_tuple->data, *(InTypeRight*)table_data.data());
        }
      }
    }

    // compute right queue against left table
    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<InTypeRight> *in_right_tuple =(Tuple<InTypeRight> *)(in_data_right);
      // get all matching on data from right
      for (auto &[table_data, table_idx] : this->tuple_to_index_left) {
        for (auto it = this->index_to_deltas_left_[table_idx].begin();
             it != this->index_to_deltas_left_[table_idx].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
          out_tuple->delta = {std::max(in_right_tuple->delta.ts, it->ts),
                              in_right_tuple->delta.count * it->count};
          
		  out_tuple->data =  this->join_layout_(*(InTypeLeft*)table_data.data(), in_right_tuple->data);
        }
      }
    }

 
    // insert new deltas from in_queues
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_left.contains(
              Key<InTypeLeft>(in_left_tuple->data))) {
        index match_index = this->next_index_left_;
        this->next_index_left_++;
        this->tuple_to_index_left[Key<InTypeLeft>(in_left_tuple->data)] =
            match_index;
        this->index_to_deltas_left_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_left_tuple->delta});
	  } else {
        index match_index =
            this->tuple_to_index_left[Key<InTypeLeft>(in_left_tuple->data)];
        this->index_to_deltas_left_[match_index].insert(in_left_tuple->delta);
      }
    }

    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<InTypeRight> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_right.contains(
              Key<InTypeRight>(in_right_tuple->data))) {
        index match_index = this->next_index_right_;
        this->next_index_right_++;
        this->tuple_to_index_right[Key<InTypeRight>(in_right_tuple->data)] =
            match_index;
        this->index_to_deltas_right_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_right_tuple->delta});
      } else {
        index match_index =
            this->tuple_to_index_left[Key<InTypeRight>(in_right_tuple->data)];
        this->index_to_deltas_left_[match_index].insert(in_right_tuple->delta);
      }
    }

    // clean in_queues
    this->in_queue_left_->Clean();
    this->in_queue_right_->Clean();

    if (this->compact_) {
      this->Compact();
    }
  }

private:
  // only state beyound StatefulNode state is join_layout_function
  std::function<OutType(const InTypeLeft&, const InTypeRight&)> join_layout_;
};

// join on
// match type could be even char[] in this case
template <typename InTypeLeft, typename InTypeRight, typename MatchType,
          typename OutType>
class JoinNode : public StatefulBinaryNode<InTypeLeft, InTypeRight, OutType> {
public:
  JoinNode(
      Node *in_node_left, Node *in_node_right,
      std::function<MatchType(const InTypeLeft &)> get_match_left,
      std::function<MatchType(const InTypeRight &)> get_match_right,
  	  std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout)
      : StatefulBinaryNode<InTypeLeft, InTypeRight, OutType>(in_node_left,
                                                             in_node_right),
        get_match_left_(get_match_left), get_match_right_{get_match_right},
        join_layout_{join_layout} {}

  // this function changes
  void Compute() {

    // compute right_queue against left_queue
    // they are small and hold no indexes so we do it just by nested loop
    const char *in_data_left;
    const char *in_data_right;
    char *out_data;
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      while (this->in_queue_right_->GetNext(&in_data_right)) {
        Tuple<InTypeRight> *in_right_tuple =
            (Tuple<InTypeRight> *)(in_data_right);
        // if left and right queues match on data put it into out_queue with new
        // delta
        if (this->Compare(&in_left_tuple->data, &in_right_tuple->data)) {
          this->out_queue_->ReserveNext(&out_data);
          Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
          out_tuple->delta = {
              std::max(in_left_tuple->delta.ts, in_right_tuple->delta.ts),
              in_left_tuple->delta.count * in_right_tuple->delta.count};
           
		     out_tuple->data = this->join_layout_(in_left_tuple->data, in_right_tuple->data);
        }
      }
    }

    // compute left queue against right table
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      // get all matching on data from right
      MatchType match = this->get_match_left_(in_left_tuple->data);
      // all tuples from right table that match this left tuple
      for (auto tpl_it =
               this->match_to_tuple_right_[Key<MatchType>(match)].begin();
           tpl_it != this->match_to_tuple_right_[Key<MatchType>(match)].end();
           tpl_it++) {
        // now iterate all version of this tuple
        std::array<char, sizeof(InTypeLeft)> left_key = *tpl_it;
        int idx = this->tuple_to_index_right[left_key];

        for (auto it = this->index_to_deltas_right_[idx].begin();
             it != this->index_to_deltas_right_[idx].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
          out_tuple->delta = {std::max(in_left_tuple->delta.ts, it->ts),
                              in_left_tuple->delta.count * it->count};
           out_tuple->data =  this->join_layout_(in_left_tuple->data,
                             *reinterpret_cast<InTypeRight*>(left_key.data()));
        }
      }
    }
    // compute right queue against left table
    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<InTypeLeft> *in_right_tuple = (Tuple<InTypeRight> *)(in_data_right);
      // get all matching on data from right
      MatchType match = this->get_match_right_(in_right_tuple->data);
      // all tuples from right table that match this left tuple
      for (auto tpl_it =
               this->match_to_tuple_left_[Key<MatchType>(match)].begin();
           tpl_it != this->match_to_tuple_left_[Key<MatchType>(match)].end();
           tpl_it++) {
        // now iterate all version of this tuple
        std::array<char, sizeof(InTypeRight)> right_key = *tpl_it;
        int idx = this->tuple_to_index_left[right_key];

        for (auto it = this->index_to_deltas_left_[idx].begin();
             it != this->index_to_deltas_left_[idx].end(); it++) {
          this->out_queue_->ReserveNext(&out_data);
          Tuple<OutType> *out_tuple = (Tuple<OutType> *)(out_data);
          out_tuple->delta = {std::max(in_right_tuple->delta.ts, it->ts),
                              in_right_tuple->delta.count * it->count};
          out_tuple->data =  this->join_layout_(*reinterpret_cast<InTypeLeft*>(right_key.data()),in_right_tuple->data);
        }
      }
    }

    // insert new deltas from in_queues
    while (this->in_queue_left_->GetNext(&in_data_left)) {
      Tuple<InTypeLeft> *in_left_tuple = (Tuple<InTypeLeft> *)(in_data_left);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_left.contains(
              Key<InTypeLeft>(in_left_tuple->data))) {
        index match_index = this->next_index_left_;
        this->next_index_left_++;
        this->tuple_to_index_left[Key<InTypeLeft>(in_left_tuple->data)] =
            match_index;
        this->index_to_deltas_left_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_left_tuple->delta});
        // also insert matching for this tuple
        MatchType left_match = this->get_match_left_(in_left_tuple->data);
        if (!this->match_to_tuple_left_.contains(Key<MatchType>(left_match))) {
          this->match_to_tuple_left_[Key<MatchType>(left_match)] =
              std::list<std::array<char, sizeof(InTypeLeft)>>{};
        }
        this->match_to_tuple_left_[Key<MatchType>(left_match)].push_back(
            Key<InTypeLeft>(in_left_tuple->data));

      } else {
        index match_index =
            this->tuple_to_index_left[Key<InTypeLeft>(in_left_tuple->data)];
        this->index_to_deltas_left_[match_index].insert(in_left_tuple->delta);
      }
    }

    while (this->in_queue_right_->GetNext(&in_data_right)) {
      Tuple<InTypeRight> *in_right_tuple =
          (Tuple<InTypeRight> *)(in_data_right);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index_right.contains(
              Key<InTypeRight>(in_right_tuple->data))) {
        index match_index = this->next_index_right_;
        this->next_index_right_++;
        this->tuple_to_index_right[Key<InTypeRight>(in_right_tuple->data)] =
            match_index;
        this->index_to_deltas_right_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_right_tuple->delta});

        // also insert matching for this tuple
        MatchType right_match = this->get_match_right_(in_right_tuple->data);
        if (!this->match_to_tuple_right_.contains(
                Key<MatchType>(right_match))) {
          this->match_to_tuple_right_[Key<MatchType>(right_match)] =
              std::list<std::array<char, sizeof(InTypeRight)>>{};
        }
        this->match_to_tuple_right_[Key<MatchType>(right_match)].push_back(
            Key<InTypeRight>(in_right_tuple->data));
      }

      else {
        index match_index =
            this->tuple_to_index_left[Key<InTypeRight>(in_right_tuple->data)];
        this->index_to_deltas_left_[match_index].insert(in_right_tuple->delta);
      }
    }

    // clean in_queues
    this->in_queue_left_->Clean();
    this->in_queue_right_->Clean();

    if (this->compact_) {
      this->Compact();
    }
  }

private:
  inline bool Compare(InTypeLeft *left_data, InTypeRight *right_data) {
    MatchType left_match = this->get_match_left_(*left_data);
    MatchType right_match = this->get_match_right_(*right_data);
    return !std::memcmp(&left_match, &right_match, sizeof(MatchType));
  }

  // we need those functions to calculate matchfields from tuples on left and
  // righ
  std::function<MatchType(const InTypeLeft &)> get_match_left_;
  std::function<MatchType(const InTypeRight &)> get_match_right_;
  std::function<OutType(const InTypeLeft &, const InTypeRight &)> join_layout_;


  // we need to get corresponding tuples using only match chars, this maps will
  // help us with it
  std::unordered_map<std::array<char, sizeof(MatchType)>,
                     std::list<std::array<char, sizeof(InTypeLeft)>>,
                     KeyHash<MatchType>>
      match_to_tuple_left_;
  std::unordered_map<std::array<char, sizeof(MatchType)>,
                     std::list<std::array<char, sizeof(InTypeRight)>>,
                     KeyHash<MatchType>>
      match_to_tuple_right_;
};

/**
 * OK for this node we might want to use bit different model of computation
 * it will only emit single value at right time, and it need to delete old
 * value, it also need to compute next value based not only on data but also on
 * delta
 */

// and finally aggregate_by, so we are aggregating but also with grouping by
// fields what if we would store in type and out type, and then for all tuples
// would emit new version from their oldest version

template <typename InType, typename MatchType, typename OutType>
class AggregateByNode : public Node {
  // now output of aggr might be also dependent of count of in tuple, for
  // example i sum inserting 5 Alice's should influence it different than one
  // Alice but with min it should not, so it will be dependent on aggregate
  // function
public:
  AggregateByNode(Node *in_node,
                  std::function<void(OutType *, InType *, int)> aggr_fun,
                  std::function<void(InType *, MatchType *)> get_match)
      : in_node_{in_node}, in_queue_{in_node->Output()},
        frontier_ts_{in_node->GetFrontierTs()}, aggr_fun_{aggr_fun},
        get_match_{get_match} {
    this->ts_ = get_current_timestamp();
    // there will be single tuple emited at once probably, if not it will get
    // resized so chill
    this->out_queue_ = new Queue(2, sizeof(Tuple<OutType>));
  }

  Queue *Output() { return this->out_queue_; }

  timestamp GetFrontierTs() const {return this->frontier_ts_;}
  
  std::vector<Node *> Inputs() { return {this->in_node_}; }

  // output should be single tuple with updated values for different times
  // so what we can do there? if we emit new count as part of value, then it
  // will be treated as separate tuple if we emit new value as part of delta it
  // also will be wrong what if we would do : insert previous value with count
  // -1 and insert current with count 1 at the same time then old should get
  // discarded
  void Compute() {
    // insert new deltas from in_queues
    const char *in_data;
    while (this->in_queue_->GetNext(&in_data)) {
      Tuple<InType> *in_tuple = (Tuple<InType> *)(in_data);
      // if this data wasn't present insert with new index
      if (!this->tuple_to_index.contains(Key<InType>(in_tuple->data))) {
        index match_index = this->next_index_;
        this->next_index_++;
        this->tuple_to_index[Key<InType>(in_tuple->data)] = match_index;
        this->index_to_deltas_.emplace_back(
            std::multiset<Delta, DeltaComparator>{in_tuple->delta});

  
  
        MatchType match;
        this->get_match_(&in_tuple->data, &match);
        if (!this->match_to_tuple_.contains(Key<MatchType>(match))) {
          this->match_to_tuple_[Key<MatchType>(match)] =
              std::list<std::array<char, sizeof(InType)>>{};
        }
        this->match_to_tuple_[Key<MatchType>(match)].push_back(
            Key<InType>(in_tuple->data));
      } else {
        index match_index = this->tuple_to_index[Key<InType>(in_tuple->data)];
        this->index_to_deltas_[match_index].insert(in_tuple->delta);
      }
    }

    if (this->compact_) {
	  // emit delete for oldest keept version, emit insert for previous_ts

      // iterate by match type
      for (auto it = this->match_to_tuple_.begin(); it != this->match_to_tuple_.end(); it++) {
        OutType accum = this->initial_value_;
		if(this->emited_.contains(it->first)){
			for (auto data_it = it->second.begin(); data_it != it->second.end();
				data_it++) {
			int index = this->tuple_to_index[*data_it];
			aggr_fun_(&accum, reinterpret_cast<InType *>(data_it->data()),
						this->index_to_deltas_[index].rbegin()->count);
			}

			// emit delete tuple
			char *out_data;
			this->out_queue_->ReserveNext(&out_data);
			Tuple<OutType> *del_tuple = (Tuple<OutType> *)(out_data);
			del_tuple->delta = {this->previous_ts_, -1};
			std::memcpy(&del_tuple->data, &accum, sizeof(OutType));
		}else{
			this->emited_.insert(it->first);
		}
	  }

      this->Compact();

      this->compact_ = false;

      // iterate by match type this time we will insert, after compaction prev
      // index should be lst value and we want to insert it
      for (auto it = this->match_to_tuple_.begin();
           it != this->match_to_tuple_.end(); it++) {
        OutType accum = this->initial_value_;

        for (auto data_it = it->second.begin(); data_it != it->second.end();
             data_it++) {
          int index = this->tuple_to_index[*data_it];
          aggr_fun_(&accum, reinterpret_cast<InType *>(data_it->data()),
                     this->index_to_deltas_[index].rbegin()->count);
        }

        // emit delete tuple
        char *out_data;
        this->out_queue_->ReserveNext(&out_data);
        Tuple<OutType> *ins_tuple = (Tuple<OutType> *)(out_data);
        ins_tuple->delta = {this->previous_ts_, 1};      
		std::memcpy(&ins_tuple->data, &accum, sizeof(OutType));
	  }
    }
  }

  void UpdateTimestamp(timestamp ts) {
    if (this->ts_ + this->frontier_ts_ < ts) {
      this->previous_ts_ = this->ts_;
      this->ts_ = ts;
      this->compact_ = true;
      this->in_node_->UpdateTimestamp(ts);
    }
  }

private:
  // let's store full version data and not deltas in this node, so we can just
  // discard old versions
  void Compact() {
    // compact all the way to previous version, we need it to to emit delete
    compact_deltas(this->index_to_deltas_, this->previous_ts_);
  }

  // timestamp will be used to track valid tuples
  // after update propagate it to input nodes

  size_t next_index_ = 0;

  bool compact_ = false;
  timestamp ts_;
  timestamp previous_ts_;

  timestamp frontier_ts_;

  Node *in_node_;

  Queue *in_queue_;

  Queue *out_queue_;

  std::unordered_map<std::array<char, sizeof(InType)>, index, KeyHash<InType>>
      tuple_to_index;
  
  
  std::vector<std::multiset<Delta, DeltaComparator>> index_to_deltas_;
  std::unordered_map<std::array<char, sizeof(MatchType)>,
                     std::list<std::array<char, sizeof(InType)>>,
                     KeyHash<MatchType>>
      match_to_tuple_;

  std::function<void(InType *, MatchType *)> get_match_;
  std::function<void(OutType *, InType *, int)> aggr_fun_;
  OutType initial_value_;

  std::set<std::array<char, sizeof(MatchType)>> emited_;


  
  std::mutex node_mutex;
};

} // namespace AliceDB
#endif
