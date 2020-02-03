# mlr3db 0.1.4

* Connections can now be automatically re-connected via a user-provided function.
* `DataBackendDplyr` now has a finalizer which automatically disconnects the
  data base connection during garbage collection.

# mlr3db 0.1.3

* During construction of `DataBackendDplyr`, you can now select columns to be
  converted from string to factor. This simplies the work with SQL databases
  which do not naturally support factors (or where the level information is
  lost in the transaction).

# mlr3db 0.1.2

* Fixed `$distinct()` to not return missing values per default.
* Added `na_rm` argument to `$distinct()`.
* Renamed `as_sqlite()` to `as_sqlite_backend()`

# mlr3db 0.1.1

* Initial release.
