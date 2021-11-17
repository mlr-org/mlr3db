recode = function(tab, levels) {
  if (length(levels) > 0L) {
    for (col in intersect(names(tab), names(levels))) {
      set(tab, i = NULL, j = col, value = factor(tab[[col]], levels = levels[[col]]))
    }
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

  file.path(parent, sprintf("%s.%s",
    gsub("[^[:alnum:]._-]", "_", hash),
    extension)
  )
}
