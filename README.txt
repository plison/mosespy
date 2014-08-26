
==============================================
	MosesPy
	Version 0.2
	
	Main developer:	Pierre Lison 
					(plison@ifi.uio.no)
					
	URL: http://mosespy.googlecode.com
	
	Released under an MIT license
==============================================


INTRODUCTION:
=============

The MosesPy interface is a Python package designed to ease the use of the
Moses toolkit for statistical machine translation.  It contains a set of
methods for training, tuning and evaluating SMT models.

The interface relies on 3 third-party tools: the Moses toolkit itself
(and its set of processing scripts), the multi-threaded GIZA (MGIZA++),
and the IRSTLM language modelling toolkit.

MosesPy is released under an MIT license. Note that this license only 
applies to the MosesPy package, not the third-party tools!


INSTALL:
=============

In order to compile the code for these libraries, simply run the 'install.py'
script:
	$ python -m mosespy.install
	
Note that the installation script will only work for reasonably standard
configurations. If the script fails, you will have to compile the code
manually (see the instructions on the Moses website and the documentation
for MGIZA++ and IRSTLM for details).

RUN:
====

Once Moses, MGIZA++ and IRSTLM are compiled, you are ready to configure
and run translation experiments.  A simple example of experiment is
shown in the file run_experiment.py.  Of course, you will need to download 
the data to run the experiment:
 * the training data is available at
 	[ http://www.statmt.org/wmt13/training-parallel-nc-v8.tgz ]
 * the tuning and testing data are available at 
 	[ http://www.statmt.org/wmt12/dev.tgz ]
 
 As you might notice, this experiment is a replica of the baseline setup
 described in http://www.statmt.org/moses/?n=Moses.Baseline, but is 
 substantially easier to run.  The final BLEU score at the end of this
 experiment example should be around 23.50.
 
 
MORE INFO:
==========

You will soon find more documentation on the MosesPy website:
	http://mosespy.googlecode.com
	
Stay tuned! 
 
 
 