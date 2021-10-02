sudo service ssh start

mkdir ~/.ssh/
sudo cp ~/ssh-keys/id_rsa* ~/.ssh/
sudo chown -R ubuntu:ubuntu ~/.ssh/*
chmod -R 600 ~/.ssh/*

cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys

export LOCALE_IPS="$(python3 /opt/client/arkouda/integration.py 'arkouda' 'arkouda-locale')"
export SSH_SERVERS="$MY_IP $LOCALE_IPS"

./arkouda_server -nl ${NUMLOCALES:-1} --memTrack=${MEMTRACK:-true} --authenticate=${AUTHENTICATE:-false} \
                 --logLevel=${LOG_LEVEL:-LogLevel.INFO}
                                                           
