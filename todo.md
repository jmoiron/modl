Todo:

- cache/store as much reflect stuff as possible
- add query builder
- update docs with new examples
- add better interfaces to control underlying types to TableMap

In Progress:

- remove list & new struct support form in favor of filling pointers and slices (removed in Get, not Select)
- replace reflect struct filling with structscan from sqlx (done in Get, not Select)

Done:

- use strings.ToLower on table & field names by default, aligning behavior w/ sqlx
- replace hook calling process with one that uses interfaces

