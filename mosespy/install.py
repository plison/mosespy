# -*- coding: utf-8 -*- 

# =================================================================                                                                   
# Copyright (C) 2014-2017 Pierre Lison (plison@ifi.uio.no)
                                                                            
# Permission is hereby granted, free of charge, to any person 
# obtaining a copy of this software and associated documentation 
# files (the "Software"), to deal in the Software without restriction, 
# including without limitation the rights to use, copy, modify, merge, 
# publish, distribute, sublicense, and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, 
# subject to the following conditions:

# The above copyright notice and this permission notice shall be 
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# =================================================================  


"""This module is used to install (i.e. compile) the third-party tools
employed by MosesPy -- namely Moses, MGIZA++ and IRSTLM.  It also
defines the directory paths for these tools, and a set of default
values used through MosesPy.  

Please note that the compilation methods defined here will only work
in their default configuration.  For more advanced compilation settings
(for instance, if the boost library are not in the library path),
you will have to compile these tools manually. 
    
"""

__author__ = 'Pierre Lison (plison@ifi.uio.no)'
__copyright__ = 'Copyright (c) 2014-2017 Pierre Lison'
__license__ = 'MIT License'
__version__ = "$Date:: 2014-08-26 08:44:16 #$"


import mosespy.system as system
from mosespy.system import Path

rootPath = Path(__file__).getAbsolute().getUp().getUp()
expDir = rootPath + "/experiments/"
moses_root = Path(rootPath + "/mosesdecoder")
mgizapp_root = Path(rootPath + "/mgiza")
irstlm_root = Path(rootPath + "/irstlm")

decoder = moses_root+"/bin/moses"

defaultAlignment = "grow-diag-final-and"
defaultReordering = "msd-bidirectional-fe"



def compile_moses():
    """Compile the Moses binaries (assuming the boost libraries can be found
    in the library path).
    
    """
    compileCmd = " ./bjam -j8 -q"
    result = system.run("cd " + moses_root + "; " + compileCmd)
    if not result:
        raise RuntimeError("Compilation of Moses has failed.  Please compile "
                           + " Moses manually (see http://statmt/moses).")
    
def compile_mgizapp():
    """Compile the binaries for multi-threaded GIZA (assuming the boost 
    libraries can be found in the library path).
    
    """    
    result1 = system.run("cd " + mgizapp_root + "; cmake .")
    if not result1:
        raise RuntimeError("Use of CMake for MGIZA++ has failed.  Please compile "
                           + "MGIZA++ manually (see program documentation).")
    result2 = system.run("cd " + mgizapp_root + "; make ")
    if not result2:
        raise RuntimeError("Use of 'make' for MGIZA++ has failed.  Please compile "
                           + "MGIZA++ manually (see program documentation).")
    result3 = system.run("cd " + mgizapp_root + "; make install")
    if not result3:
        raise RuntimeError("Use of 'make install' for MGIZA++ has failed.  Please compile "
                           + "MGIZA++ manually (see program documentation).")
    Path(mgizapp_root+"/scripts/merge_alignment.py").copy(mgizapp_root + "/bin")
                         

def compile_irstlm():
    """Compile the binaries for the IRSTLM language modelling toolkit.
    
    """
    result1 = system.run("cd " + irstlm_root + "; ./regenerate-makefiles.sh")
    if not result1:
        raise RuntimeError("Generation of Makefiles for IRSTLM has failed. Please "
                           + "compile IRSTLM manually (see program documentation).")
    configureCmd = "./configure --prefix " + irstlm_root.getAbsolute()
    result2 = system.run("cd " + irstlm_root + " ; " + configureCmd)
    if not result2:
        raise RuntimeError("Use of 'configure' for IRSTLM has failed.  Please "
                           + "compile IRSTLM manually (see program documentation).")
    result3 = system.run("cd " + irstlm_root + "; make ")
    if not result3:
        raise RuntimeError("Use of 'make' for IRSTLM has failed.  Please compile "
                           + "IRSTLM manually (see program documentation).")
    result4 = system.run("cd " + irstlm_root + "; make install")
    if not result4:
        raise RuntimeError("Use of 'make install' for IRSTLM has failed.  Please "
                           + "compile IRSTLM manually (see program documentation).")


if __name__ == "__main__":
    """Compiles the code.
    
    """
    compile_moses()
    compile_mgizapp()
    compile_irstlm()
