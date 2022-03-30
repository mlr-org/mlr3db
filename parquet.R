library(DBI)
devtools::load_all()

files = list.files("inst/extdata", full.names = TRUE)
files = sprintf("parquet_scan(['%s'])", paste0(files, collapse = "','"))

con = dbConnect(duckdb::duckdb())
dbGetQuery(con, sprintf("select row_number() over () as id from %s", files))
DBI::dbExecute(con,
  sprintf('CREATE UNIQUE INDEX primary_key ON "%s" ("%s")', files, "id"))
dbGetQuery(con, sprintf("select * from %s", files))


dbGetQuery(con, sprintf("select email, row_number() over () as id from '%s' where id < 10", files))

dbGetQuery(con, sprintf("PRAGMA table_info(%s)", files))

dbExecute(con, sprintf("CREATE VIEW merged AS SELECT * FROM %s", files))
dbGetQuery(con, sprintf("PRAGMA table_info('merged')"))



con = dbConnect(duckdb::duckdb())
files = list.files("inst/extdata", full.names = TRUE)
b = DataBackendDuckDB$new(con, files, NULL)
