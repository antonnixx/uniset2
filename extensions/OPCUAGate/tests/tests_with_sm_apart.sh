#!/bin/sh

# '--' - нужен для отделения аргументов catch, от наших..
cd ../../../Utilities/Admin/
./uniset2-start.sh -f ./create_links.sh
./uniset2-start.sh -f ./create

./uniset2-start.sh -f ./exist | grep -q UNISET_PLC/Controllers || exit 1
cd -

./uniset2-start.sh -f ./tests-with-sm -- --apart --confile opcuagate-test-configure.xml --e-startup-pause 10 \
--opcua-s-filter-field iotype --smemory-id SharedMemory

# --opcua-log-add-levels any