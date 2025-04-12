//===----------------------------------------------------------------------===//
//                         Yahoo Finance
//
// functions/functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {
namespace yahoo_finance {

struct First {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct Last {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct TimeBucket {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct Aliases {
  static void Register(Connection &conn, Catalog &catalog);
};

} // namespace yahoo_finance
} // namespace duckdb
