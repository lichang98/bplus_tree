#include "bplustree.hpp"
#include <iostream>

#include <cassert>
#include <concepts>
#include <memory>
#include <queue>
#include <utility>
#include <variant>
#include <vector>

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
using root_type_t = std::shared_ptr<BPlusTNode<Key, Val>>;

enum class NodeType {
  ROOT = 1,
  MIDDLE = 2,
  NON_LEAF = 3,
  LEAF = 4,
  ROOT_LEAF = 5
};

template <typename Key, typename Val>
requires NodeCpt<Key, Val>
class BPlusTNode {
public:
  BPlusTNode(NodeType type) { _type = type; }

  u32 find_pos(Key &key) {
    u32 i = 0;
    for (; i < _keys.size(); ++i) {
      if (key < _keys[i]) {
        break;
      }
    }
    return i;
  }

  u32 find_pos_exact(Key &key) {
    u32 i = 0;
    for (; i < _keys.size(); ++i) {
      if (key == _keys[i]) {
        break;
      }
    }
    return (i == _keys.size() ? -1 : i);
  }

  void leaf_insert(Key &key, Val &val, u32 pos) {
    assert(_type == NodeType::LEAF);
    _keys.insert(_keys.begin() + pos, key);
    _fields.insert(_fields.begin() + pos, val);
  }

  std::shared_ptr<BPlusTNode<Key, Val>>
  get_field_as_ptr(field_type_t<Key, Val> &field) {
    return std::get<std::shared_ptr<BPlusTNode<Key, Val>>>(field);
  }

  Val get_field_as_val(field_type_t<Key, Val> &field) {
    return std::get<Val>(field);
  }

  // Only for leaf node
  void move_right_half(std::shared_ptr<BPlusTNode<Key, Val>> other, u32 beg_pos,
                       bool is_leaf) {
    other->_keys.insert(other->_keys.begin(), _keys.begin() + beg_pos,
                        _keys.end());
    _keys.erase(_keys.begin() + beg_pos, _keys.end());
    if (is_leaf) {
      other->_fields.insert(other->_fields.begin(), _fields.begin() + beg_pos,
                            _fields.end());
      _fields.erase(_fields.begin() + beg_pos, _fields.end());
    } else {
      other->_fields.insert(other->_fields.begin(),
                            _fields.begin() + beg_pos + 1, _fields.end());
      _fields.erase(_fields.begin() + beg_pos + 1, _fields.end());
    }
  }

  // Only for leaf node
  void move_left_half(std::shared_ptr<BPlusTNode<Key, Val>> other,
                      u32 end_pos) {
    assert(_type == NodeType::LEAF && other->_type == NodeType::LEAF);
    other->_keys.insert(other->_keys.end(), _keys.begin(),
                        _keys.begin() + end_pos);
    other->_fields.insert(other->_fields.end(), _fields.begin(),
                          _fields.begin() + end_pos);
    _keys.erase(_keys.begin(), _keys.begin() + end_pos);
    _fields.erase(_fields.begin(), _fields.begin() + end_pos);
  }

  Key get_min() {
    assert(!_keys.empty());
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

  bool remove(Key key, u32 *pos_rm) {
    u32 pos = find_pos_exact(key);
    if (pos < 0) {
      return false;
    }
    _keys.erase(_keys.begin() + pos);
    *pos_rm = pos;
  }

  void remove_fld_by_pos(u32 pos) {
    assert(pos < _fields.size());
    _fields.erase(_fields.begin() + pos);
  }

  // For non-leaf nodes
  std::shared_ptr<BPlusTNode<Key, Val>> split_right() {
    std::shared_ptr<BPlusTNode<Key, Val>> right_node =
        std::make_shared<BPlusTNode<Key, Val>>(_type);
    right_node->_parent = _parent;
    move_right_half(right_node, _keys.size() / 2, _type == NodeType::LEAF);
    return right_node;
  }

  bool touch_low_limit() {
    return _keys.size() < static_cast<u32>(_node_max_cap / 2);
  }
  bool touch_high_limit() { return _keys.size() > _node_max_cap; }

  friend std::ostream &operator<<(std::ostream &out, const BPlusTNode &node) {
    u32 i = 0;
    switch (node._type) {
    case NodeType::ROOT_LEAF:
    case NodeType::LEAF:
      for (; i < node._keys.size() - 1; ++i) {
        out << "[Key=" << node._keys[i]
            << ", Val=" << std::get<Val>(node._fields[i]) << "], ";
      }
      out << "[Key=" << node._keys[i]
          << ", Val=" << std::get<Val>(node._fields[i]) << "]  ";
      break;
    default:
      for (; i < node._keys.size() - 1; ++i) {
        out << "[Key=" << node._keys[i] << "], ";
      }
      out << "[Key=" << node._keys[i] << "]  ";
      break;
    }
    return out;
  }

  std::weak_ptr<BPlusTNode> _parent;
  std::shared_ptr<BPlusTNode> _leaf_right;
  std::vector<Key> _keys;
  node_fields_t<Key, Val> _fields;
  NodeType _type;

  static constexpr u16 _node_max_cap = 3;
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
    std::shared_ptr<BPlusTNode<Key, Val>> p = _root;
    while (p->_type != NodeType::LEAF) {
      u32 pos = p->find_pos(key);
      assert(pos >= 0);
      p = std::get<std::shared_ptr<BPlusTNode<Key, Val>>>(p->_fields[pos]);
      assert(p);
    }
    u32 pos = p->find_pos(key);
    p->leaf_insert(key, val, pos);
    if (!p->touch_high_limit())
      return;

    // When target node reaching high limit
    // Split and copy right min to parent, the parent
    // may also reaching high limit after that, repeating
    // (move right min to parent instead of copying) until root
    // or current parent space enough.
    std::shared_ptr<BPlusTNode<Key, Val>> right_leaf = p->split_right();
    right_leaf->_leaf_right = p->_leaf_right;
    p->_leaf_right = right_leaf;
    Key right_min = right_leaf->get_min();
    std::shared_ptr<BPlusTNode<Key, Val>> par = p->_parent.lock();
    std::shared_ptr<field_type_t<Key, Val>> push_val =
        std::make_shared<field_type_t<Key, Val>>(right_leaf);
    par->insert(right_min, push_val);
    right_leaf->_parent = par;
    p = par;
    par = p->_parent.lock();
    while (p != _root && p->touch_high_limit()) {
      std::shared_ptr<BPlusTNode<Key, Val>> right_node = p->split_right();
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

    // Cascade split reaching root
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
  void remove(Key key);
  bool search(Key key);

  void visit_leaves() {
    std::shared_ptr<BPlusTNode<Key, Val>> p = _leaf_beg;
    while (p) {
      std::cout << *p << " --- ";
      p = p->_leaf_right;
    }
    std::cout << std::endl;
  }

  void level_visit() {
    std::queue<std::shared_ptr<BPlusTNode<Key, Val>>> que;
    que.push(_root);
    u32 i = 1;
    std::queue<u32> splits;
    splits.push(1);
    u32 j = 0;
    while (!que.empty()) {
      if (que.front()->_type != NodeType::LEAF) {
        splits.push(que.front()->_fields.size());
        for (auto child : que.front()->_fields) {
          que.push(std::get<std::shared_ptr<BPlusTNode<Key, Val>>>(child));
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

  root_type_t<Key, Val> _root;
  std::shared_ptr<BPlusTNode<Key, Val>> _leaf_beg;
  u32 _height;
};

int main(int argc, char **argv) {
  BPlusTree<u32, u32> tree;

  std::cout << "insert 1,2001" << std::endl;
  tree.insert(1, 2001);

  std::cout << "insert 2,2404" << std::endl;
  tree.insert(2, 2404);

  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "insert 4,99187" << std::endl;
  tree.insert(4, 99187);

  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "insert 5,88177" << std::endl;
  tree.insert(5, 88177);

  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  tree.level_visit();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 3,10000" << std::endl;
  tree.insert(3, 10000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 0,40000" << std::endl;
  tree.insert(0, 40000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 9,90000" << std::endl;
  tree.insert(9, 90000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 7,70000" << std::endl;
  tree.insert(7, 70000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 8,80000" << std::endl;
  tree.insert(8, 80000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 6,60000" << std::endl;
  tree.insert(6, 60000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();

  std::cout << "level vis" << std::endl;
  std::cout << "insert 11,110000" << std::endl;
  tree.insert(11, 110000);
  tree.level_visit();
  std::cout << "vis leafs" << std::endl;
  tree.visit_leaves();
  return 0;
}
