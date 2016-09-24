export EXTERNAL_ROUTER="localhost:8080"
export INTERNAL_ROUTER="localhost:8080"
export EXTERNAL_SCHEME="http"

source local-export-pg-connection-variables.sh
#source renew-tokens.sh
python test.py