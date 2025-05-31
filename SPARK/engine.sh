#!/bin/bash
#assign variable
echo "job_id : $1 "
echo "nip : $2 "
echo "acct  : $3 "
echo "start_date : $4 "
echo "end_date : $5 "
echo "type : $6 "

#kinit user
kinit -kt /home/nifi/cml2.keytab cmluser@CLOUDEKA.AI


$run engine document
spark3-submit   --master yarn   --deploy-mode cluster  --name "$1_$2_$3_$4_$5_$6"  --jars /home/nifi/phoenix5-spark3-shaded-6.0.0.3.3.7190.0-91.jar,/home/nifi/pqs719.jar   /home/nifi/rk.py $1 $2 $3 $4 $5 $6