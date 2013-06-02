Todo:

- benchmarks that can compare mainline gorp to this fork
- cache/store as much reflect stuff as possible
- add query builder
- update docs with new examples
- add better interfaces to control underlying types to TableMap

In Progress:

Done:

- remove list & new struct support form in favor of filling pointers and slices
- replace reflect struct filling with structscan from sqlx
- use strings.ToLower on table & field names by default, aligning behavior w/ sqlx
- replace hook calling process with one that uses interfaces

