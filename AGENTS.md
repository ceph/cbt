## Before writing code

Read docs/dev/coding-guidelines.md and follow it. It covers the typing, architecture-boundary,
validation, testing, and commit conventions.

Write unit tests *before* writing code that is for a central feature, fixing a bug, or handling
complex edge-cases, thereby implementing TDD.

## Navigating the codebase

Read STRUCTURE.md for a complete map of every git-tracked source file.
Consult it instead of running `ls`, `find`, or `list_files` to orient yourself.

Keep STRUCTURE.md up to date. Whenever a source file is, moved, renamed, or deleted, update 
the corresponding entry in STRUCTURE.md in the same commit.