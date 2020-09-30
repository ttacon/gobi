gobi
====

# Development
To develop in this repo, clone it down and then run: 
```
git submodule init
go mod download
# pkger is used for managing bee-queue lua scripts.
go get github.com/markbates/pkger/cmd/pkger
pkger
```
(you'll want go 1.15+).
