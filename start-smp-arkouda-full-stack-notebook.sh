#!/bin/bash

nohup ./arkouda_server -nl ${NUMLOCALES:-1} > nohup.out 2> nohup.err < /dev/null &

jupyter notebook --ip=0.0.0.0 --allow-root --no-browser
                                                           
