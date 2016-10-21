export EXTERNAL_ROUTER_HOST="localhost"
export EXTERNAL_ROUTER_PORT="8080"
export INTERNAL_ROUTER_HOST="localhost"
export INTERNAL_ROUTER_PORT="8080"
export EXTERNAL_SCHEME="http"

source local-export-pg-connection-variables.sh
#source renew-tokens.sh
python test.py