recode = function(tab, levels) {
  for (col in intersect(names(tab), names(levels))) {
    set(tab, i = NULL, j = col, value = factor(tab[[col]], levels = levels[[col]]))
  }
  tab[]
}
