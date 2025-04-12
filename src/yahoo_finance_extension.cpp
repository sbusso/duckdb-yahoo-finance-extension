#define DUCKDB_EXTENSION_MAIN

#include "yahoo_finance_extension.hpp"
#include "functions/functions.hpp"
#include "functions/scanner.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <iostream>
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

void YahooFinanceExtension::Load(DuckDB &db) {
  Connection con(db);
  con.BeginTransaction();

  auto &catalog = Catalog::GetSystemCatalog(*con.context);
  yahoo_finance::First::RegisterFunction(con, catalog);
  yahoo_finance::Last::RegisterFunction(con, catalog);
  yahoo_finance::TimeBucket::RegisterFunction(con, catalog);
  yahoo_finance::Aliases::Register(con, catalog);

  // Create Yahoo Scanner Function
  TableFunction yahoo_scanner("yahoo_finance",
                              {LogicalType::ANY, LogicalType::ANY,
                               LogicalType::ANY, LogicalType::VARCHAR},
                              yahoo_finance::YahooScanner::Scan,
                              yahoo_finance::YahooScanner::Bind);
  CreateTableFunctionInfo yahoo_scanner_info(yahoo_scanner);
  catalog.CreateTableFunction(*con.context, &yahoo_scanner_info);

  // Create Portfolio Frontier Function
  // FIXME: this should not be dependent of the yahoo scanner
  // TableFunction portfolio_frontier(
  //     "portfolio_frontier",
  //     {duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
  //      LogicalType::ANY, LogicalType::ANY, LogicalType::INTEGER},
  //     scrooge::PortfolioFrontier::Scan, scrooge::PortfolioFrontier::Bind);
  // CreateTableFunctionInfo portfolio_frontier_info(portfolio_frontier);
  // catalog.CreateTableFunction(*con.context, &portfolio_frontier_info);


  con.Commit();
}

std::string YahooFinanceExtension::Name() { return "yahoo_finance"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void yahoo_finance_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::YahooFinanceExtension>();
}

DUCKDB_EXTENSION_API const char *yahoo_finance_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}