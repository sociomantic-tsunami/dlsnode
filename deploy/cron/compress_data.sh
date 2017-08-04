#!/bin/bash

set -e

DIR="/srv/dlsnode/data/"
DAYS_TO_COMPRESS=90
DAYS_TO_DELETE=397

# compress files older than 'uncompressed_days'
find "$DIR" ! -name "*\.gz" ! -name "checkpoint\.dat*" ! -name "sizeinfo" -type f -daystart -mtime +$DAYS_TO_COMPRESS -exec ionice -c 3 gzip -f {} \;

# delete compressed files older than 410 days (to be able to compare current month with same month last year and have some margin)
find "$DIR" -name "*\.gz" -type f -daystart -mtime +$DAYS_TO_DELETE -delete

# remove empty dirs
find "$DIR" -type d -empty -delete

