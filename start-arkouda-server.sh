sudo service ssh start
export LOCALE_IPS=$(python arkouda/integration.py 'namespace="arkouda"' \
                 'app_name="arkouda-locale"' 'pretty_print=True')
export SSH_SERVERS=$MY_IP $LOCALE_IPS
./arkouda_server -nl ${NUMLOCALES:-1} --memTrack=${MEMTRACK:-true} --authenticate=${AUTHENTICATE:-false} \
                 --logLevel=${LOG_LEVEL:-LogLevel.INFO}
