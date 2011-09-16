
if [ ! $IRSTLM ] ; then echo "IRSTLM variable is not set" ; exit ; fi
if [ ! -d $IRSTLM ] ; then echo "IRSTLM variable ($IRSTLM) is not a valid path" ; exit ; fi

bindir=$IRSTLM/bin


$bindir/compile-lm train.en-micro.blm --eval test.en-micro
$bindir/compile-lm train.en.blm --eval test.en
$bindir/compile-lm train.micro.blm --eval test.micro
$bindir/compile-lm train.macro.blm --eval test.macro

$bindir/compile-lm train.lmmacro.macro --eval test.en-micro
$bindir/compile-lm train.lmmacro.header-en --eval test.en-micro
$bindir/compile-lm train.lmmacro.header-micro --eval test.en-micro

$bindir/interpolate-lm interpolateLM.cfg --eval test.en-micro
