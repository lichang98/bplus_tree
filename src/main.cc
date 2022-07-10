#include "bplustree.hpp"
#include "thread_pool.hpp"
#include <iostream>

#include <gtest/gtest.h>
#include <map>

using namespace bplustree;

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
  std::vector<u32> vals(500000);
  for (u32 i = 0; i < 500000; ++i) {
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

TEST(BPlusTreeTest, NormalRangeTest) {
  BPlusTree<u32, u32> tree;
  constexpr u32 sz = 50;
  std::vector<u32> vals;
  std::vector<u32> vals2;
  for (u32 i = 10; i < sz + 10; ++i) {
    vals.emplace_back(i);
    vals2.emplace_back(i);
  }

  for (auto &x : vals) {
    tree.insert(x, x);
  }

  auto ret = tree.search_range(0, 10);
  EXPECT_EQ(ret.size(), 0);

  ret = tree.search_range(13, 59);
  EXPECT_EQ(ret.size(), 46);

  ret = tree.search_range(13, 52);
  EXPECT_EQ(ret.size(), 39);

  ret = tree.search_range(15, 59);
  EXPECT_EQ(ret.size(), 44);

  ret = tree.search_range(15, 52);
  EXPECT_EQ(ret.size(), 37);

  ret = tree.search_range(16, 59);
  EXPECT_EQ(ret.size(), 43);

  ret = tree.search_range(16, 52);
  EXPECT_EQ(ret.size(), 36);

  ret = tree.search_range(50, 70);
  EXPECT_EQ(ret.size(), 10);

  ret = tree.search_range(50, 70);
  EXPECT_EQ(ret.size(), 10);

  ret = tree.search_range(50, 70);
  EXPECT_EQ(ret.size(), 10);
}

TEST(BPlusTreeTest, RWRangeTest) {
  BPlusTree<u32, u32> tree;
  constexpr u32 sz = 5000;
  std::vector<u32> vals;
  std::vector<u32> vals2;
  for (u32 i = 0; i < sz; ++i) {
    vals.emplace_back(i);
    vals2.emplace_back(i);
  }

  for (auto &x : vals) {
    tree.insert(x, x);
  }

  for (u32 i = 500; i < 1000; ++i) {
    tree.remove(i);
  }
  for (u32 i = 1300; i < 1900; ++i) {
    tree.remove(i);
  }

  auto ret = tree.search_range(0, 600);
  EXPECT_EQ(ret.size(), 500);

  ret = tree.search_range(500, 600);
  EXPECT_EQ(ret.size(), 0);

  ret = tree.search_range(400, 1200);
  EXPECT_EQ(ret.size(), 300);

  ret = tree.search_range(1400, 1800);
  EXPECT_EQ(ret.size(), 0);

  ret = tree.search_range(1400, 2000);
  EXPECT_EQ(ret.size(), 100);

  ret = tree.search_range(400, 6000);
  EXPECT_EQ(ret.size(), 3500);
}

TEST(BPlusTreeTest, NormalRandomRWTest) {
  std::map<u32, u32> mp; // For checking
  auto rng = std::default_random_engine(time(0));
  constexpr u32 sz = 500000;
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
