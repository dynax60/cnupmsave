#!/bin/sh

CNUPM_RSYNC_DIR=~rsync/cnupmsave/
CNUPM_SAVE_SCRIPT=/usr/local/cnupmsave/cnupmsave.pl

cd ${CNUPM_RSYNC_DIR} || exit 1
for i in `find . -name \*.dump`
do
	cidr=`echo $i | sed -e 's/\-.*$//; s/\,/\//; s/\.\///;'`
	${CNUPM_SAVE_SCRIPT} -e $i ${cidr}
done
