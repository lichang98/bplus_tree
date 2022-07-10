Bplus Tree with C++ template
----

## Introduction

These codes implement a bplus tree with c++ template for
general purpose use. It uses some new features up to C++20.

Since it is a full-in-memory one, which differs from normal
db that exchanges pages between memory and disk. Thus, simple
parallelism mechanism may not make improvements when operations
are fast. Tests for simple parallel method is disabled default,
uncomment *add_compile_definitions* line in CMakeLists.txt 
to enable related macros.

## Usage

`mkdir build; cd build; cmake ..; make`

The *thread pool* codes are cloned from [https://github.com/progschj/ThreadPool](https://github.com/progschj/ThreadPool)
