#!/bin/sh

PATH=/bin:/usr/bin:/usr/local/bin
CNUPM_DIR=~cnupm
CNUPM_SAVE_HOST=tokyo
CNUPM_SAVE_MODULE=cnupmsave
CNUPM_SAVE_USR=cnupmsave
CNUPM_SAVE_PWD="${CNUPM_DIR}/cnupmsave.pwd"
CNUPM_SAVE_PID=/var/run/cnupmsave.pid

upload_dump()
{
	cnupm_dump="cnupm-$1.dump"
	part=$2
	f=`echo ${cnupm_dump} | sed -e "s/cnupm/${part}/"`

	if [ ! -r ${cnupm_dump} -a ! -s ${cnupm_dump} ]; then
		return
	fi

	echo -n "$f ... "
	rsync -4 --password-file=${CNUPM_SAVE_PWD} \
	    rsync://${CNUPM_SAVE_USR}@${CNUPM_SAVE_HOST}/${CNUPM_SAVE_MODULE}/ \
	    | grep -w "$f\$" >/dev/null
	if [ $? -eq 0 ]; then
		echo 'failed, not processed yet'
		return	
	fi

	rsync -4 --password-file=${CNUPM_SAVE_PWD} \
	    $cnupm_dump rsync://${CNUPM_SAVE_USR}@${CNUPM_SAVE_HOST}/${CNUPM_SAVE_MODULE}/$f
	if [ $? -ne 0 ]; then
		return
	else
		echo 'ok'
		rm -f ${cnupm_dump}
	fi
}

if [ -f ${CNUPM_SAVE_PID} ]; then
	if kill -0 `head ${CNUPM_SAVE_PID} 2>/dev/null`; then
		echo "I'm already synchronizing. Exiting." >&2
		exit 1
	fi
	rm -f ${CNUPM_SAVE_PID}
fi

echo $$ > ${CNUPM_SAVE_PID}
trap "rm -f ${CNUPM_SAVE_PID}" 1 2 3 15 EXIT

cd $CNUPM_DIR || exit 1
sync

upload_dump eth1 192.168.145.0,24

kill -HUP `cat ${CNUPM_DIR}/cnupm-*.pid`
