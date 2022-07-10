#include "bplustree.hpp"
#include "thread_pool.hpp"
#include <iostream>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <concepts>
#include <cstring>
#include <ctime>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <map>

using u8 = unsigned char;
using u16 = unsigned short;
using u32 = uint32_t;
using u64 = uint64_t;

using i8 = char;
using i16 = short;
using i32 = int32_t;
using i64 = int64_t;

template <typename Key>
concept Comparable = requires(std::remove_reference_t<Key> &u,
                              std::remove_reference_t<Key> &v) {
  { u == v } -> std::same_as<bool>;
  { u < v } -> std::same_as<bool>;
  { u > v } -> std::same_as<bool>;
};

template <typename Key, typename Val>
concept NodeCpt =
    Comparable<Key> && std::copyable<Val> && std::equality_comparable<Val>;

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class BPlusTNode;

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class BPlusTree;

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
using node_fields_t =
    std::vector<std::variant<std::shared_ptr<BPlusTNode<Key, Val>>, Val>>;

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
using field_type_t = std::variant<std::shared_ptr<BPlusTNode<Key, Val>>, Val>;

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
using node_type_t = std::shared_ptr<BPlusTNode<Key, Val>>;

enum class NodeType {
  ROOT = 1,
  MIDDLE = 2,
  NON_LEAF = 3,
  LEAF = 4,
  ROOT_LEAF = 5
};

enum class Redist { NONE = 0, LEFT, RIGHT };

enum class ReqType { QUERY = 0, INSERT, DELETE };

constexpr u16 _node_max_cap = 3;

// #define USING_PARALLEL

#ifdef USING_PARALLEL
#define SL_TREE(mut) std::shared_lock<decltype(mut)> lk(mut);
#define SU_TREE(mut) mut.unlock_shared();
#define SL_LEAF(mut) mut.lock_shared();
#define SU_LEAF(mut) mut.unlock_shared();

#define XL_TREE(mut) std::unique_lock<decltype(mut)> lk(mut);
#define XU_TREE(mut) mut.unlock();
#define XL_LEAF(mut) mut.lock();
#define XU_LEAF(mut) mut.unlock();
#else
#define SL_TREE(mut)
#define SU_TREE(mut)
#define SL_LEAF(mut)
#define SU_LEAF(mut)

#define XL_TREE(mut)
#define XU_TREE(mut)
#define XL_LEAF(mut)
#define XU_LEAF(mut)
#endif

constexpr bool too_few(u32 n) { return n < (_node_max_cap / 2); }

constexpr bool too_much(u32 n) { return n > (_node_max_cap); }

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class Request {
public:
  Request(ReqType req_type, Key key) : _req_type(req_type), _key(key) {}
  Request(ReqType req_type, Key key, Val val)
      : _req_type(req_type), _key(key), _val(val) {}
  ReqType _req_type;
  Key _key;
  Val _val;

  std::ostringstream _msg;
};

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class BPlusTNode {
public:
  BPlusTNode(NodeType type) { _type = type; }

  u32 find_pos(Key &key) {
    i32 i = 0;
    for (; i < _keys.size(); ++i) {
      if (key < _keys[i]) {
        break;
      }
    }
    return i;
  }

  u32 find_pos_low(Key &key) {
    i32 i = _keys.size() - 1;
    for (; i > 0; --i) {
      if (key >= _keys[i]) {
        break;
      }
    }
    return i;
  }

  u32 find_pos_exact(Key &key) {
    i32 i = 0;
    for (; i < _keys.size(); ++i) {
      if (key == _keys[i]) {
        break;
      }
    }
    return i;
  }

  void leaf_insert(Key &key, Val &val, u32 pos) {
    assert(_type == NodeType::LEAF);
    _keys.insert(_keys.begin() + pos, key);
    _fields.insert(_fields.begin() + pos, val);
  }

  node_type_t<Key, Val> get_field_as_ptr(field_type_t<Key, Val> &field) {
    return std::get<node_type_t<Key, Val>>(field);
  }

  Val get_field_as_val(field_type_t<Key, Val> &field) {
    return std::get<Val>(field);
  }

  // Only for leaf node
  void move_right_half(node_type_t<Key, Val> other, u32 beg_pos, bool is_leaf) {
    other->_keys.insert(other->_keys.begin(), _keys.begin() + beg_pos,
                        _keys.end());
    _keys.erase(_keys.begin() + beg_pos, _keys.end());
    if (is_leaf || _fields.size() < 2) {
      other->_fields.insert(other->_fields.begin(), _fields.begin() + beg_pos,
                            _fields.end());
      if (!is_leaf) {
        for (i32 pos = 0; pos < other->_fields.size(); ++pos) {
          std::get<node_type_t<Key, Val>>(other->_fields[pos])->_parent = other;
        }
      }
      _fields.erase(_fields.begin() + beg_pos, _fields.end());
    } else {
      other->_fields.insert(other->_fields.begin(),
                            _fields.begin() + beg_pos + 1, _fields.end());
      for (i32 pos = 0; pos < other->_fields.size(); ++pos) {
        std::get<node_type_t<Key, Val>>(other->_fields[pos])->_parent = other;
      }
      _fields.erase(_fields.begin() + beg_pos + 1, _fields.end());
    }
  }

  // Only for leaf node
  void move_left_half(node_type_t<Key, Val> other, u32 end_pos) {
    other->_keys.insert(other->_keys.end(), _keys.begin(),
                        _keys.begin() + end_pos);
    _keys.erase(_keys.begin(), _keys.begin() + end_pos);
    bool is_leaf = static_cast<u32>(_type) & static_cast<u32>(NodeType::LEAF);
    if (is_leaf || _fields.size() < 2) {
      other->_fields.insert(other->_fields.end(), _fields.begin(),
                            _fields.begin() + end_pos);
      if (!is_leaf) {
        for (i32 pos = 0; pos < other->_fields.size(); ++pos) {
          std::get<node_type_t<Key, Val>>(other->_fields[pos])->_parent = other;
        }
      }
      _fields.erase(_fields.begin(), _fields.begin() + end_pos);
    } else {
      other->_fields.insert(other->_fields.end(), _fields.begin(),
                            _fields.begin() + end_pos + 1);
      for (i32 pos = 0; pos < other->_fields.size(); ++pos) {
        std::get<node_type_t<Key, Val>>(other->_fields[pos])->_parent = other;
      }
      _fields.erase(_fields.begin(), _fields.begin() + end_pos + 1);
    }
  }

  Key get_min() {
    if (_keys.empty())
      return _min_key;
    else
      return _keys[0];
  }

  void remove_min() {
    assert(_type != NodeType::LEAF);
    assert(!_keys.empty());
    _keys.erase(_keys.begin());
  }

  // Insertion for non-leaf node(key and ptr) or leaf node(key and val)
  void insert(Key key, std::shared_ptr<field_type_t<Key, Val>> val,
              std::shared_ptr<field_type_t<Key, Val>> val2 = nullptr) {
    if (_keys.empty() &&
        !(static_cast<u32>(_type) & static_cast<u32>(NodeType::LEAF))) {
      assert(val2);
      _keys.emplace_back(key);
      _fields.emplace_back(*val);
      _fields.emplace_back(*val2);
    } else {
      assert(!val2);
      u32 pos = find_pos(key);
      _keys.insert(_keys.begin() + pos, key);
      _fields.insert(_fields.begin() + pos +
                         ((static_cast<u32>(_type) &
                           static_cast<u32>(NodeType::LEAF)) == 0),
                     *val);
    }
  }

  void update_split_key(node_type_t<Key, Val> new_key, Redist redist) {
    assert(_type != NodeType::LEAF);
    switch (redist) {
    case Redist::LEFT:
      for (i32 i = 0; i < _keys.size(); ++i) {
        if (new_key->get_min() <= _keys[i]) {
          _keys[i] = new_key->get_min();
          break;
        }
      }
      break;
    case Redist::RIGHT:
      for (i32 i = _keys.size() - 1; i >= 0; --i) {
        if (new_key->get_min() >= _keys[i]) {
          _keys[i] = new_key->get_min();
          break;
        }
      }
      break;
    default:
      break;
    }
  }

  bool remove(Key key, u32 *pos_rm) {
    u32 pos = find_pos_exact(key);
    if (pos > _keys.size())
      return false;
    if (_keys.size() == 1)
      _min_key = _keys[0];
    _keys.erase(_keys.begin() + pos);
    *pos_rm = pos;
    return true;
  }

  void remove_key_by_pos(u32 pos) {
    assert(pos < _keys.size());
    if (_keys.size() == 1)
      _min_key = _keys[0];

    _keys.erase(_keys.begin() + pos);
  }

  void remove_fld_by_pos(u32 pos) {
    assert(pos < _fields.size());
    _fields.erase(_fields.begin() + pos);
  }

  // For non-leaf nodes
  node_type_t<Key, Val> split_right() {
    node_type_t<Key, Val> right_node =
        std::make_shared<BPlusTNode<Key, Val>>(_type);
    right_node->_parent = _parent;
    move_right_half(right_node, _keys.size() / 2, _type == NodeType::LEAF);
    return right_node;
  }

  node_type_t<Key, Val> get_right_sib() {
    assert(!(static_cast<u32>(_type) & static_cast<u32>(NodeType::ROOT)));
    u32 pos = 0;
    auto par = _parent.lock();
    while (pos < par->_fields.size() &&
           std::get<node_type_t<Key, Val>>(par->_fields[pos])->get_min() !=
               get_min())
      ++pos;
    assert(pos < par->_fields.size());
    if (pos == par->_fields.size() - 1)
      return nullptr;
    else
      return std::get<node_type_t<Key, Val>>(par->_fields[pos + 1]);
  }

  node_type_t<Key, Val> get_left_sib() {
    assert(!(static_cast<u32>(_type) & static_cast<u32>(NodeType::ROOT)));
    u32 pos = 0;
    auto par = _parent.lock();
    while (pos < par->_fields.size() &&
           std::get<node_type_t<Key, Val>>(par->_fields[pos])->get_min() !=
               get_min())
      ++pos;
    assert(pos < par->_fields.size());
    if (pos == 0)
      return nullptr;
    else
      return std::get<node_type_t<Key, Val>>(par->_fields[pos - 1]);
  }

  bool touch_low_limit() {
    return _keys.size() < static_cast<u32>(_node_max_cap / 2);
  }
  bool touch_high_limit() { return _keys.size() > _node_max_cap; }

  friend std::ostream &operator<<(std::ostream &out, const BPlusTNode &node) {
    i32 i = 0;
    switch (node._type) {
    case NodeType::ROOT_LEAF:
    case NodeType::LEAF:
      if (node._keys.empty())
        break;
      for (; i < node._keys.size() - 1; ++i) {
        out << "[Key=" << node._keys[i]
            << ", Val=" << std::get<Val>(node._fields[i]) << "]";
        if (node._parent.lock()) {
          out << "(isleaf, parent key0=" << node._parent.lock()->_keys[0]
              << "), ";
        }
      }
      out << "[Key=" << node._keys[i]
          << ", Val=" << std::get<Val>(node._fields[i]) << "]";
      if (node._parent.lock()) {
        out << "(isleaf, parent key0=" << node._parent.lock()->_keys[0]
            << ")  ";
      }
      break;
    default:
      if (node._keys.empty())
        break;
      for (; i < node._keys.size() - 1; ++i) {
        out << "[Key=" << node._keys[i] << "]";
        if (node._parent.lock()) {
          out << "(parent key0=" << node._parent.lock()->_keys[0] << ")  ";
        }
      }
      out << "[Key=" << node._keys[i] << "]";
      if (node._parent.lock()) {
        out << "(parent key0=" << node._parent.lock()->_keys[0] << ")  ";
      }
      break;
    }
    out << " ###### ";
    return out;
  }

  std::weak_ptr<BPlusTNode> _parent;
  std::shared_ptr<BPlusTNode> _leaf_right;
  std::shared_ptr<BPlusTNode> _leaf_left;
  std::vector<Key> _keys;
  node_fields_t<Key, Val> _fields;
  Key _min_key;
  NodeType _type;
};

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class BPlusTree {
public:
  BPlusTree() {
    _root = std::make_shared<BPlusTNode<Key, Val>>(NodeType::ROOT_LEAF);
    _height = 0;
    _leaf_beg = _root;
  }

  void insert(Key key, Val val) {
    XL_TREE(_mut_rw)
    Val tmp_val;
    auto tmp_p = search(key, tmp_val, false);
    if (tmp_p) {
      // If exists, update with new val
      assert(static_cast<u32>(tmp_p->_type) & static_cast<u32>(NodeType::LEAF));
      u32 tmp_pos = tmp_p->find_pos_exact(key);
      tmp_p->_fields[tmp_pos] = val;
      return;
    }

    // When only root node
    if (_height == 0) {
      std::shared_ptr<field_type_t<Key, Val>> val_entry(
          new field_type_t<Key, Val>(val));
      _root->insert(key, val_entry);
      if (_root->touch_high_limit()) {
        // Split two child, copy right node min val to parent
        auto &left = _root;
        left->_type = NodeType::LEAF;
        auto right = std::make_shared<BPlusTNode<Key, Val>>(NodeType::LEAF);
        auto new_root = std::make_shared<BPlusTNode<Key, Val>>(NodeType::ROOT);
        u32 size = left->_keys.size();
        left->move_right_half(right, left->_keys.size() / 2,
                              left->_type == NodeType::LEAF);
        left->_leaf_right = right;
        right->_leaf_left = left;
        _leaf_beg = left;
        left->_parent = new_root;
        right->_parent = new_root;
        // Copy right min val to parent
        new_root->insert(right->get_min(),
                         std::make_shared<field_type_t<Key, Val>>(left),
                         std::make_shared<field_type_t<Key, Val>>(right));
        _root = new_root;
        ++_height;
      }
      return;
    }
    assert(_root && _root->_type != NodeType::ROOT_LEAF);
    // When space of target node is enough
    auto p = _root;
    while (p->_type != NodeType::LEAF) {
      u32 pos = p->find_pos(key);
      assert(pos >= 0);
      p = std::get<node_type_t<Key, Val>>(p->_fields[pos]);
      assert(p);
    }
    u32 pos = p->find_pos(key);
    p->leaf_insert(key, val, pos);
    if (!p->touch_high_limit()) {
      return;
    }

    // When target node reaching high limit
    // Split and copy right min to parent, the parent
    // may also reaching high limit after that, repeating
    // (move right min to parent instead of copying) until root
    // or current parent space enough.
    auto right_leaf = p->split_right();
    right_leaf->_leaf_right = p->_leaf_right;
    p->_leaf_right = right_leaf;
    right_leaf->_leaf_left = p;
    if (right_leaf->_leaf_right)
      right_leaf->_leaf_right->_leaf_left = right_leaf;
    Key right_min = right_leaf->get_min();
    auto par = p->_parent.lock();
    auto push_val = std::make_shared<field_type_t<Key, Val>>(right_leaf);
    par->insert(right_min, push_val);
    right_leaf->_parent = par;
    p = par;
    par = p->_parent.lock();
    while (p != _root && p->touch_high_limit()) {
      auto right_node = p->split_right();
      Key right_min = right_node->get_min();
      push_val = std::make_shared<field_type_t<Key, Val>>(right_node);
      par->insert(right_min, push_val);
      right_node->remove_min();

      p = par;
      if (p == _root) {
        break;
      }
      par = p->_parent.lock();
    }

    // Cascade spliting reaches root
    if (p->touch_high_limit() && p == _root) {
      p->_type = NodeType::MIDDLE;
      auto right = std::make_shared<BPlusTNode<Key, Val>>(NodeType::MIDDLE);
      auto new_root = std::make_shared<BPlusTNode<Key, Val>>(NodeType::ROOT);
      p->move_right_half(right, p->_keys.size() / 2,
                         p->_type == NodeType::LEAF);

      p->_parent = new_root;
      right->_parent = new_root;
      _root = new_root;
      _root->insert(right->get_min(),
                    std::make_shared<field_type_t<Key, Val>>(p),
                    std::make_shared<field_type_t<Key, Val>>(right));
      right->remove_min();
      ++_height;
    }
  }

  void remove(Key key) {
    XL_TREE(_mut_rw);
    Val val;
    u32 pos;
    auto p = search(key, val, false);
    if (!p) {
      return;
    }

    if (p->_type == NodeType::ROOT_LEAF) {
      bool exist = p->remove(key, &pos);
      p->remove_fld_by_pos(pos);
      if (p->_keys.empty()) {
        _leaf_beg = nullptr;
      }
      return;
    }

    auto sib = p->get_right_sib();
    bool is_right = true;
    if (!sib) {
      is_right = false;
      sib = p->get_left_sib();
    }

    assert(sib);
    Key right_min = is_right ? sib->get_min() : p->get_min();

    bool exist = p->remove(key, &pos);
    p->remove_fld_by_pos(pos);
    if (!p->touch_low_limit()) {
      return;
    }
    auto parent = p->_parent.lock();

    while (p != _root && too_few(p->_keys.size())) {
      // Redistribute
      if (too_much(p->_keys.size() + sib->_keys.size())) {
        u32 total_size = p->_keys.size() + sib->_keys.size();
        u32 mv_size = sib->_keys.size() - total_size / 2 - (total_size % 2);
        if (is_right) {
          pos = parent->find_pos_low(right_min);
          assert(pos < parent->_keys.size());
          if (sib->_type != NodeType::LEAF) {
            p->_keys.emplace_back(parent->_keys[pos]);
          }
          sib->move_left_half(p, mv_size);
          parent->update_split_key(sib, Redist::RIGHT);
          if (sib->_type != NodeType::LEAF) {
            sib->_keys.erase(sib->_keys.begin());
          }
        } else {
          pos = parent->find_pos_low(right_min);
          assert(pos < parent->_keys.size());
          if (sib->_type != NodeType::LEAF) {
            sib->_keys.emplace_back(parent->_keys[pos]);
          }
          sib->move_right_half(p, total_size / 2, p->_type == NodeType::LEAF);
          parent->update_split_key(p, Redist::LEFT);
          if (p->_type != NodeType::LEAF) {
            p->_keys.erase(p->_keys.begin());
          }
        }
      } else {
        // Merge with sibling, remove from parent the split key,
        // the removing may cascade
        if (is_right) {
          pos = parent->find_pos_low(right_min);
          assert(pos < parent->_keys.size());
          if (p->_type != NodeType::LEAF) {
            p->_keys.emplace_back(parent->_keys[pos]);
          }

          parent->remove_key_by_pos(pos);
          parent->remove_fld_by_pos(pos);
          p->move_right_half(sib, 0, p->_type == NodeType::LEAF);
          parent->update_split_key(sib, Redist::NONE);
          if (p == _leaf_beg) {
            _leaf_beg = sib;
          } else {
            if (p->_leaf_left) {
              p->_leaf_left->_leaf_right = sib;
            }
            sib->_leaf_left = p->_leaf_left;
          }
        } else {
          pos = parent->find_pos_low(right_min);
          assert(pos < parent->_keys.size());
          if (sib->_type != NodeType::LEAF) {
            p->_keys.insert(p->_keys.begin(), parent->_keys[pos]);
          }
          parent->remove_key_by_pos(pos);
          parent->remove_fld_by_pos(pos + 1);
          p->move_left_half(sib, p->_keys.size());
          parent->update_split_key(sib, Redist::NONE);

          sib->_leaf_right = p->_leaf_right;
          if (p->_leaf_right) {
            p->_leaf_right->_leaf_left = sib;
          }
        }
        if (parent == _root && parent->_keys.empty()) {
          _root = sib;
          _root->_parent.reset();
          _root->_type = _root->_type == NodeType::LEAF ? NodeType::ROOT_LEAF
                                                        : NodeType::ROOT;
          --_height;
          break;
        }
      }
      p = parent;
      parent = p->_parent.lock();
      if (p != _root) {
        sib = p->get_right_sib();
        is_right = true;
        if (!sib) {
          is_right = false;
          sib = p->get_left_sib();
        }

        assert(sib);
        right_min = is_right ? sib->get_min() : p->get_min();
      }
    }
  }

  node_type_t<Key, Val> search(Key key, Val &ret_val, bool need_lock = true) {
    if (need_lock) {
      SL_TREE(_mut_rw)
    }
    auto p = _root;
    while (p &&
           !(static_cast<u32>(p->_type) & static_cast<u32>(NodeType::LEAF))) {
      u32 pos = p->find_pos(key);
      assert(pos < p->_fields.size());
      p = std::get<node_type_t<Key, Val>>(p->_fields[pos]);
    }
    assert(p);
    u32 pos = p->find_pos_exact(key);
    if (pos >= p->_keys.size()) {
      return nullptr;
    }
    ret_val = std::get<Val>(p->_fields[pos]);
    return p;
  }

  void visit_leaves() {
    auto p = _leaf_beg;
    if (!p) {
      std::cout << " --- " << std::endl;
    }
    while (p) {
      std::cout << *p;
      p = p->_leaf_right;
    }
    std::cout << std::endl;
  }

  void level_visit() {
    if (_root->_keys.empty()) {
      std::cout << "[ ]" << std::endl;
      return;
    }
    std::queue<node_type_t<Key, Val>> que;
    que.push(_root);
    u32 i = 1;
    std::queue<u32> splits;
    splits.push(1);
    u32 j = 0;
    while (!que.empty()) {
      if (!(static_cast<u32>(que.front()->_type) &
            static_cast<u32>(NodeType::LEAF))) {
        splits.push(que.front()->_fields.size());
        for (auto child : que.front()->_fields) {
          que.push(std::get<node_type_t<Key, Val>>(child));
        }
      }
      std::cout << *(que.front());
      que.pop();
      --i;
      ++j;
      if (j == splits.front()) {
        j = 0;
        splits.pop();
      }
      if (i == 0) {
        i = que.size();
        std::cout << "\n=======================" << std::endl;
      }
    }
  }

  node_type_t<Key, Val> _root;
  node_type_t<Key, Val> _leaf_beg;
  u32 _height;

  std::shared_mutex _mut_rw;
};

TEST(BPlusTreeTest, DISABLED_NormalTest) {
  BPlusTree<u32, u32> tree;

  u32 val;

  std::cout << "insert 1,2001" << std::endl;
  tree.insert(1, 2001);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "insert 2,2404" << std::endl;
  tree.insert(2, 2404);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "insert 4,99187" << std::endl;
  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  tree.insert(4, 99187);

  auto ret = tree.search(1, val);
  assert(ret && val == 2001);

  ret = tree.search(-1, val);
  assert(!ret);

  ret = tree.search(5, val);
  assert(!ret);

  ret = tree.search(4, val);
  assert(ret && val == 99187);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "insert 5,88177" << std::endl;
  tree.insert(5, 88177);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  tree.level_visit();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 3,10000" << std::endl;
  tree.insert(3, 10000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 0,40000" << std::endl;
  tree.insert(0, 40000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 9,90000" << std::endl;
  tree.insert(9, 90000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 7,70000" << std::endl;
  tree.insert(7, 70000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 8,80000" << std::endl;
  tree.insert(8, 80000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 6,60000" << std::endl;
  tree.insert(6, 60000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 11,110000" << std::endl;
  tree.insert(11, 110000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "tree root key size=" << tree._root->_keys.size()
            << ", key 0 = " << tree._root->_keys[0]
            << ", tree height=" << tree._height << std::endl;
  val = 0;
  ret = tree.search(11, val);
  assert(ret && val == 110000);
  ret = tree.search(1, val);
  assert(ret && val == 2001);
  ret = tree.search(2, val);
  assert(ret && val == 2404);
  ret = tree.search(3, val);
  assert(ret && val == 10000);
  ret = tree.search(0, val);
  assert(ret && val == 40000);
  ret = tree.search(9, val);
  assert(ret && val == 90000);
  ret = tree.search(7, val);
  assert(ret && val == 70000);
  ret = tree.search(8, val);
  assert(ret && val == 80000);
  ret = tree.search(6, val);
  assert(ret && val == 60000);
  ret = tree.search(123, val);
  assert(!ret);
  ret = tree.search(-2, val);
  assert(!ret);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=4" << std::endl;
  tree.remove(4);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=6" << std::endl;
  tree.remove(6);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=5" << std::endl;
  tree.remove(5);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=11" << std::endl;
  tree.remove(11);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=9" << std::endl;
  tree.remove(9);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=8" << std::endl;
  tree.remove(8);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "remove key=7" << std::endl;
  tree.remove(7);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();
  std::cout << "tree root key size=" << tree._root->_keys.size()
            << ", key 0 = " << tree._root->_keys[0]
            << ", tree height=" << tree._height << std::endl;
  std::cout << "\n++++++++++++++++++++++++++" << std::endl;

  std::cout << "remove key=0" << std::endl;
  tree.remove(0);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "remove key=1" << std::endl;
  tree.remove(1);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "remove key=2" << std::endl;
  tree.remove(2);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "remove key=3" << std::endl;
  tree.remove(3);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "insert 1,2001" << std::endl;
  tree.insert(1, 2001);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "insert 2,2404" << std::endl;
  tree.insert(2, 2404);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "insert 4,99187" << std::endl;
  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  tree.insert(4, 99187);

  ret = tree.search(1, val);
  assert(ret && val == 2001);

  ret = tree.search(-1, val);
  assert(!ret);

  ret = tree.search(5, val);
  assert(!ret);

  ret = tree.search(4, val);
  assert(ret && val == 99187);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "insert 5,88177" << std::endl;
  tree.insert(5, 88177);

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  tree.level_visit();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 3,10000" << std::endl;
  tree.insert(3, 10000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 0,40000" << std::endl;
  tree.insert(0, 40000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 9,90000" << std::endl;
  tree.insert(9, 90000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 7,70000" << std::endl;
  tree.insert(7, 70000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 8,80000" << std::endl;
  tree.insert(8, 80000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 6,60000" << std::endl;
  tree.insert(6, 60000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "\n++++++++++++++++++++++++++" << std::endl;
  std::cout << "level vis" << std::endl;
  std::cout << "insert 11,110000" << std::endl;
  tree.insert(11, 110000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "tree root key size=" << tree._root->_keys.size()
            << ", key 0 = " << tree._root->_keys[0]
            << ", tree height=" << tree._height << std::endl;
}

TEST(BPlusTreeTest, DISABLED_NormalBigTest) {
  BPlusTree<u32, u32> tree;
  constexpr u32 sz = 5000000;
  for (u32 i = 0; i < sz; ++i) {
    tree.insert(i, i);
  }

  u32 val;
  auto start = std::chrono::system_clock::now();
  for (u32 i = 0; i < sz; ++i) {
    EXPECT_NE(tree.search(i, val), nullptr);
    EXPECT_EQ(val, i);
  }
  auto end = std::chrono::system_clock::now();
  std::cout << "Searching using "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << " ms" << std::endl;

  for (u32 i = 0; i < sz; ++i) {
    tree.remove(i);
    EXPECT_EQ(tree.search(i, val), nullptr);
  }
  for (u32 i = 0; i < sz; ++i) {
    EXPECT_EQ(tree.search(i, val), nullptr);
  }
}

TEST(BPlusTreeTest, DISABLED_ParallelBigTest) {
  ThreadPool pool(4);
  BPlusTree<u32, u32> tree;
  constexpr u32 sz = 5000000;
  std::vector<
      std::future<decltype(std::declval<BPlusTree<u32, u32>>().insert(0, 0))>>
      futs;
  futs.reserve(sz);

  for (u32 i = 0; i < sz; ++i) {
    auto ret = pool.enqueue(&BPlusTree<u32, u32>::insert, &tree, i, i);
    futs.emplace_back(std::move(ret));
  }

  u32 val;
  for (u32 i = 0; i < sz; ++i) {
    futs[i].wait();
  }

  std::vector<std::future<decltype(std::declval<BPlusTree<u32, u32>>().search(
      0, val, true))>>
      futs2;
  futs2.reserve(sz);

  auto start = std::chrono::system_clock::now();
  for (u32 i = 0; i < sz; ++i) {
    auto ret = pool.enqueue(&BPlusTree<u32, u32>::search, &tree, i, val, false);
    futs2.emplace_back(std::move(ret));
  }

  for (u32 i = 0; i < sz; ++i) {
    futs2[i].wait();
  }

  auto end = std::chrono::system_clock::now();
  std::cout << "Searching using "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << " ms" << std::endl;

  for (u32 i = 0; i < sz; ++i) {
    tree.remove(i);
    EXPECT_EQ(tree.search(i, val), nullptr);
  }
  for (u32 i = 0; i < sz; ++i) {
    EXPECT_EQ(tree.search(i, val), nullptr);
  }
}

TEST(BPlusTreeTest, NormalRandomTest) {
  auto rng = std::default_random_engine(time(0));
  std::vector<u32> vals(5000);
  for (u32 i = 0; i < 5000; ++i) {
    vals[i] = i;
  }
  std::shuffle(vals.begin(), vals.end(), rng);

  BPlusTree<u32, u32> tree;
  for (auto &i : vals) {
    tree.insert(i, i);
  }

  u32 val;
  for (auto &i : vals) {
    EXPECT_NE(tree.search(i, val), nullptr);
    EXPECT_EQ(val, i);
  }

  for (auto &i : vals) {
    tree.remove(i);
    EXPECT_EQ(tree.search(i, val), nullptr);
  }

  for (auto &i : vals) {
    EXPECT_EQ(tree.search(i, val), nullptr);
  }
}

TEST(BPlusTreeTest, NormalRandomRWTest) {
  std::map<u32, u32> mp; // For checking
  auto rng = std::default_random_engine(time(0));
  constexpr u32 sz = 50000;
  std::vector<u32> vals(sz);
  std::vector<u32> vals2(sz);
  for (u32 i = 0; i < sz; ++i) {
    vals[i] = i;
    vals2[i] = i;
  }
  std::shuffle(vals.begin(), vals.end(), rng);
  std::shuffle(vals2.begin(), vals2.end(), rng);

  BPlusTree<u32, u32> tree;
  u32 val;

  for (i32 i = 0; i < sz; ++i) {
    tree.remove(vals[i]);
    mp.erase(vals[i]);
    EXPECT_EQ(tree.search(vals[i], val), nullptr);
    tree.insert(vals2[i], vals2[i]);
    mp[vals2[i]] = vals2[i];
    EXPECT_NE(tree.search(vals2[i], val), nullptr);
  }

  for (auto x : vals) {
    if (mp.find(x) == mp.end()) {
      EXPECT_EQ(tree.search(x, val), nullptr);
    } else {
      EXPECT_NE(tree.search(x, val), nullptr);
    }
  }

  for (auto x : vals2) {
    if (mp.find(x) == mp.end()) {
      EXPECT_EQ(tree.search(x, val), nullptr);
    } else {
      EXPECT_NE(tree.search(x, val), nullptr);
    }
  }

  for (auto x : vals) {
    tree.remove(x);
    EXPECT_EQ(tree.search(x, val), nullptr);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
