recode = function(tab, levels) {
  for (col in intersect(names(tab), names(levels))) {
    set(tab, i = NULL, j = col, value = factor(tab[[col]], levels = levels[[col]]))
  }
  tab[]
}

get_db_path = function(path, hash, extension) {
  assert_string(path)
  parent = switch(path,
    ":temp:" = tempdir(),
    ":user:" = R_user_dir("mlr3db", "cache"),
    path
  )
  if (!dir.exists(parent)) {
    dir.create(parent, recursive = TRUE)
  }

  hash = gsub("[^[:alnum:]._-]", "_", hash)
  file.path(parent, sprintf("%s.%s", hash, extension))
}

hash_data_frame = function(x) {
  if (is.data.table(x)) {
    x = as.data.frame(x)
  }
  digest::digest(x, algo = "xxhash64")
}
