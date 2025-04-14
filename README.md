## Integrity Checker

Checks integrity of files under path and saves it in database.</br>
Can perfrom later checks of integrity of files compared to database.

```
Usage: integrity-checker [OPTIONS] <--create|--check|--update|--list>

Options:
      --create
      --check
      --update
      --list
      --db <DB>               [default: files_data.redb]
      --path <PATH>...
      --exclude <EXCLUDE>...
      --dont-exclude-db
      --overwrite
  -h, --help                  Print help
  -V, --version               Print version
