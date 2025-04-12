//===----------------------------------------------------------------------===//
//                         Yahoo Finance
//
// functions/scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {
namespace yahoo_finance {
struct YahooScanner {
  static unique_ptr<FunctionData> Bind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);
  static void Scan(ClientContext &context, TableFunctionInput &data_p,
                   DataChunk &output);
};

struct PortfolioFrontier {
  static unique_ptr<FunctionData> Bind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);
  static void Scan(ClientContext &context, TableFunctionInput &data_p,
                   DataChunk &output);
};

} // namespace yahoo_finance

} // namespace duckdb
