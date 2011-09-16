bin_released_dir = example
bin_released_files = train.gz test

binrelease_cmd = \
	mkdir -p ${top_srcdir}/${bininstalldir}/${bin_released_dir} ; \
	for p in $(bin_released_files); do \
	cp -r $$p ${top_srcdir}/${bininstalldir}/${bin_released_dir}/$$p ; \
	done ;

install:
	$(binrelease_cmd)

EXTRA_DIST = \
train.gz \
test

