#!/bin/bash
set -e

echo "=== Listing migration files ==="
ls -1 migrations/*.up.sql | sort

echo ""
echo "=== Counting migration files ==="
echo "Up migrations: $(ls -1 migrations/*.up.sql | wc -l)"
echo "Down migrations: $(ls -1 migrations/*.down.sql | wc -l)"

echo ""
echo "=== Checking for duplicate version numbers ==="
ls -1 migrations/*.up.sql | cut -d'_' -f1 | cut -d'/' -f2 | sort | uniq -c | grep -v "^ *1 " || echo "No duplicates found"

echo ""
echo "=== Running sqlx migrate info ==="
sqlx migrate info || echo "Failed to get migration info"
