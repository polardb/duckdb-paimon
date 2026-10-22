# Isolated bridge qualification: host contains no statically linked Paimon.
duckdb_extension_load(paimon
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/..
)
