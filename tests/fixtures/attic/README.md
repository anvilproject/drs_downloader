# manual integration tests

```commandline


mkdir /tmp/bdc_large_file_drs_uris_20221102
mkdir /tmp/bdc_small_file_drs_uris_20221102
mkdir /tmp/kinda_big
mkdir /tmp/no-header
mkdir /tmp/small_files_not_many_uris
mkdir /tmp/smol
mkdir /tmp/smol_big
mkdir /tmp/smoller
mkdir /tmp/terra-data


drs_download terra -d /tmp/bdc_large_file_drs_uris_20221102 -m tests/fixtures/bdc_large_file_drs_uris_20221102.tsv
drs_download terra -d /tmp/bdc_small_file_drs_uris_20221102 -m tests/fixtures/bdc_small_file_drs_uris_20221102.tsv
drs_download terra -d /tmp/kinda_big -m tests/fixtures/kinda_big.tsv
drs_download terra -d /tmp/no-header -m tests/fixtures/no-header.tsv
drs_download terra -d /tmp/small_files_not_many_uris -m tests/fixtures/small_files_not_many_uris.tsv 
drs_download terra -d /tmp/smol -m tests/fixtures/smol.tsv
drs_download terra -d /tmp/smol_big -m tests/fixtures/smol_big.tsv
drs_download terra -d /tmp/smoller -m tests/fixtures/smoller.tsv
drs_download terra -d /tmp/terra-data -m tests/fixtures/terra-data.tsv


```