#! /bin/bash

set -m # Enable Job Control



function usage()
{
cmnd=$(basename $0);
cat<<EOF

$cmnd - training of a probabilistic latent semantic model

USAGE:
$cmnd [options]

OPTIONS:
-h        Show this message
-c        Collection of document e.g. 'gunzip -c docs.gz'
-m        Output model, e.g. model
-r        Model in readable format e.g. model.txt
-k        Number of splits (default 5)
-n        Number of topics (default 100)
-i        Number of training iterations (default 20)
-t        Temporary working directory (default ./stat_PID)
-d        Specific dictionary to use (optional)
-p        Pruning threshold for infrequent words (default 2)
-s        Pruning threshold for frequent words (optional)
-l        Log file (optional)
-v        Verbose

EOF
}



if [ ! $IRSTLM ]; then
echo "Set IRSTLM environment variable with path to irstlm"
exit 2
fi

#paths to scripts and commands in irstlm
scr=$IRSTLM/bin
bin=$IRSTLM/bin
gzip=`which gzip 2> /dev/null`;
gunzip=`which gunzip 2> /dev/null`;

#default parameters
tmpdir=stat_$$
data=""
topics=100
splits=5
iter=20
prunefreq=2
spectopics=0
logfile="/dev/null"
verbose=""

dict=""
model=""
txtfile="/dev/null"

while getopts "hvc:m:r:k:i:n:t:d:p:s:l:" OPTION
do
case $OPTION in
h)
usage
exit 0
;;
v)
verbose="--verbose";
;;
c)
data=$OPTARG
;;
m)
model=$OPTARG
;;
r)
txtfile=$OPTARG
;;
k)
splits=$OPTARG
;;
i)
iter=$OPTARG
;;
t)
tmpdir=$OPTARG
;;
d)
dict=$OPTARG
;;
p)
prunefreq=$OPTARG
;;
s)
spectopics=$OPTARG
;;
l)
logfile=$OPTARG
;;
?)
usage
exit 1
;;
esac
done

if [ $verbose ]; then
echo data=$data  model=$model  topics=$topics iter=$iter
logfile="/dev/stdout"
fi


if [ ! "$data" -o ! "$model" ]; then
usage
exit 5
fi

if [ -e $model ]; then
echo "Output file $model already exists! either remove or rename it."
exit 6
fi

if [ -e $txtfile -a $txtfile != "/dev/null" ]; then
echo "Output file $txtfile already exists! either remove or rename it."
exit 6
fi


if [ -e $logfile -a $logfile != "/dev/null" -a $logfile != "/dev/stdout" ]; then
echo "Logfile $logfile already exists! either remove or rename it."
exit 7
fi

#check tmpdir
tmpdir_created=0;
if [ ! -d $tmpdir ]; then
echo "Temporary directory $tmpdir does not exist";
echo "creating $tmpdir"
mkdir -p $tmpdir;
tmpdir_created=1;
else
echo "Cleaning temporary directory $tmpdir";
rm $tmpdir/* 2> /dev/null
if [ $? != 0 ]; then
echo "Warning: some temporary files could not be removed"
fi
fi


echo extract dictionary >> $logfile
$bin/dict -i=$"$data" -o=$tmpdir/dict -PruneFreq=$prunefreq -f=y >> $logfile 2>&1

echo split documents >> $logfile
$bin/plsa -c="$data" -d=$tmpdir/dict -b=$tmpdir/data -sd=$splits >> $logfile 2>&1

#rm $tmpdir/Tlist
for sp in `seq 1 1 $splits`; do echo $tmpdir/data.T.$sp >> $tmpdir/Tlist 2>&1; done
#rm $model
for it in `seq 1 1 $iter` ; do
for sp in `seq 1 1 $splits`; do
date; echo it $it split $sp
$bin/plsa -c=$tmpdir/data.$sp -d=$tmpdir/dict -st=$spectopics -hf=$tmpdir/data.H.$sp -tf=$tmpdir/data.T.$sp -wf=$model -m=$model -t=$topics -it=1 -tit=$it > /dev/null 2>&1 &
done
while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done

date; echo recombination

$bin/plsa -ct=$tmpdir/Tlist -c="$data" -d=$tmpdir/dict -hf=$tmpdir/data.H -m=$model -t=$topics -it=1 -txt=$txtfile >> $logfile 2>&1 &
done
date; echo End of training

echo "Cleaning temporary directory $tmpdir";
rm $tmpdir/* 2> /dev/null

if [ $tmpdir_created -eq 1 ]; then
echo "Removing temporary directory $tmpdir";
rmdir $tmpdir 2> /dev/null
if [ $? != 0 ]; then
echo "Warning: the temporary directory could not be removed."
fi
fi

exit 0
