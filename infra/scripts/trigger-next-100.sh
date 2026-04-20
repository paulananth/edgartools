#!/usr/bin/env bash
# Trigger bootstrap-recent-10 for the next 100 CIKs not yet loaded in Snowflake.
#
# Requirements:
#   - AWS credentials with stepfunctions:StartExecution on
#     edgartools-dev-bootstrap-recent-10
#   - Snowflake connector: pip install snowflake-connector-python
#   - Env vars: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
#              (defaults match the dev environment)
#   - EDGAR_USER_AGENT (e.g. "Your Name your.email@example.com")
#
# Usage (from Git Bash):
#   bash infra/scripts/trigger-next-100.sh
#
# Environment overrides:
#   SNOWFLAKE_DATABASE  (default EDGARTOOLS_DEV)
#   SNOWFLAKE_ROLE      (default ACCOUNTADMIN)
#   SNOWFLAKE_WAREHOUSE (default COMPUTE_WH)
#   STATE_MACHINE_ARN   (default arn:aws:states:us-east-1:690839588395:stateMachine:edgartools-dev-bootstrap-recent-10)
#   BATCH_SIZE          (default 100)

set -euo pipefail

: "${SNOWFLAKE_ACCOUNT:=GAAGJGP-CKC59844}"
: "${SNOWFLAKE_USER:?SNOWFLAKE_USER must be set}"
: "${SNOWFLAKE_PASSWORD:?SNOWFLAKE_PASSWORD must be set}"
: "${SNOWFLAKE_DATABASE:=EDGARTOOLS_DEV}"
: "${SNOWFLAKE_ROLE:=ACCOUNTADMIN}"
: "${SNOWFLAKE_WAREHOUSE:=COMPUTE_WH}"
: "${STATE_MACHINE_ARN:=arn:aws:states:us-east-1:690839588395:stateMachine:edgartools-dev-bootstrap-recent-10}"
: "${BATCH_SIZE:=100}"
: "${EDGAR_USER_AGENT:=EdgarTools dev@example.com}"

TMPDIR="${TEMP:-/tmp}"
CIK_FILE="$TMPDIR/next_ciks_$$.txt"
trap 'rm -f "$CIK_FILE"' EXIT

echo ">> Selecting next $BATCH_SIZE CIKs not yet loaded in $SNOWFLAKE_DATABASE.EDGARTOOLS_SOURCE.COMPANY"

python - <<PY > "$CIK_FILE"
import os, json, urllib.request, snowflake.connector

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
)
cur = conn.cursor()
cur.execute(f"SELECT DISTINCT CIK FROM {os.environ['SNOWFLAKE_DATABASE']}.EDGARTOOLS_SOURCE.COMPANY")
loaded = {int(r[0]) for r in cur.fetchall()}
conn.close()

req = urllib.request.Request(
    'https://www.sec.gov/files/company_tickers_exchange.json',
    headers={'User-Agent': os.environ['EDGAR_USER_AGENT']},
)
data = json.loads(urllib.request.urlopen(req, timeout=30).read())
cik_idx = data['fields'].index('cik')
universe = sorted({int(r[cik_idx]) for r in data['data']})
batch_size = int(os.environ['BATCH_SIZE'])
new = [c for c in universe if c not in loaded][:batch_size]
if len(new) < batch_size:
    import sys
    print(f"WARNING: only {len(new)} unseeded CIKs remaining", file=sys.stderr)
print(','.join(str(c) for c in new))
PY

CIK_LIST="$(cat "$CIK_FILE")"
if [[ -z "$CIK_LIST" ]]; then
  echo "ERROR: no CIKs to load (universe may already be fully covered)"
  exit 1
fi

CIK_COUNT=$(awk -F',' '{print NF}' <<< "$CIK_LIST")
RUN_ID="next${CIK_COUNT}-$(date +%Y%m%d-%H%M%S)"

echo ">> Triggering $RUN_ID with $CIK_COUNT CIKs"
echo ">> First 3: $(cut -d',' -f1-3 <<< "$CIK_LIST")"
echo ">> Last 3:  $(awk -F',' '{print $(NF-2)","$(NF-1)","$NF}' <<< "$CIK_LIST")"

MSYS_NO_PATHCONV=1 aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$RUN_ID" \
  --input "{\"cik_list\":\"$CIK_LIST\"}"

echo ""
echo ">> Monitor with:"
echo "   aws stepfunctions describe-execution \\"
echo "     --execution-arn arn:aws:states:us-east-1:690839588395:execution:edgartools-dev-bootstrap-recent-10:$RUN_ID \\"
echo "     --query 'status' --output text"
