## Integrity Checker

Checks integrity of files under path and saves it in database.</br>
Can perfrom later checks of integrity of files compared to database with --check.</br>
You can compare 2 dadabases with --compare .</br>
Can perform check of hashesh in db against [circl hashlookup](https://www.circl.lu/services/hashlookup/) with --circl-check.

```
Usage: integrity-checker [OPTIONS] <--create|--check|--update|--list|--compare|--circl-check>

Options:
      --create                creates DB and stores current files metadata
      --check                 checks current files metadata compared to DB
      --update                updates DB
      --list                  lists all files in DB
      --compare               compares 2 databases (simmilar to check)
      --circl-check           check DB against CIRCL hashes https://www.circl.lu/services/hashlookup/
      --db <DB>               [default: files_data.redb]
      --path <PATH>...        coma separated paths list
      --exclude <EXCLUDE>...  coma separated exlude paths list
      --dont-exclude-db
      --overwrite
      --db2 <DB2>             second DB for compare
      --compare-time
      --cache <CACHE>         [default: /home/<user>/.cache/cicrl_cache.redb]
  -h, --help                  Print help
  -V, --version               Print version
  ```

[![CodeQL](https://github.com/wintermute101/integrity-checker/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/wintermute101/integrity-checker/actions/workflows/github-code-scanning/codeql)