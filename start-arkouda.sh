sudo service ssh start
export LOCALE_IPS=$(python3 /opt/client/arkouda/integration.py 'arkouda' 'arkouda-locale')

export SSH_SERVERS="$LOCALE_IPS $MY_IP"

./arkouda_server -nl ${NUMLOCALES:-1} --memTrack=${MEMTRACK:-true} --authenticate=${AUTHENTICATE:-false} \
                 --logLevel=${LOG_LEVEL:-LogLevel.INFO}
