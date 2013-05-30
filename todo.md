Todo:

- remove list & new struct support form in favor of filling pointers and slices
- replace reflect struct filling with structscan from sqlx
- cache/store as much reflect stuff as possible
- add query builder
- update docs with new examples and


Almost Done:

- replace hook calling process with one that uses interfaces

Done:

- use strings.ToLower on table & field names by default, aligning behavior w/ sqlx
