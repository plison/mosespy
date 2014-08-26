
import mosespy.system as system
import mosespy.constants as constants

def compile_moses():
    compileCmd = " ./bjam -j8 -a -q"
    result = system.run("cd " + constants.moses_root + "; " + compileCmd)
    if not result:
        raise RuntimeError("Compilation of Moses has failed.  Please compile Moses"
                           + "manually (see http://statmt/moses for details).")
    
def compile_mgizapp():
    result1 = system.run("cd " + constants.mgizapp_root + "; cmake .")
    if not result1:
        raise RuntimeError("Use of CMake for MGIZA++ has failed.  Please compile MGIZA++"
                           + "manually (see program documentation for details).")
    result2 = system.run("cd " + constants.mgizapp_root + "; make ")
    if not result2:
        raise RuntimeError("Use of 'make' for MGIZA++ has failed.  Please compile MGIZA++"
                           + "manually (see program documentation for details).")
    result3 = system.run("cd " + constants.mgizapp_root + "; make install")
    if not result3:
        raise RuntimeError("Use of 'make install' for MGIZA++ has failed.  Please compile MGIZA++"
                           + "manually (see program documentation for details).")
                         

def compile_irstlm():
    result1 = system.run("cd " + constants.irstlm_root + "; ./regenerate-makefiles.sh")
    if not result1:
        raise RuntimeError("Generation of Makefiles for IRSTLM has failed.  Please compile IRSTLM"
                           + "manually (see program documentation for details).")
    configureCmd = "./configure --prefix " + constants.irstlm_root.getAbsolute()
    result2 = system.run("cd " + constants.irstlm_root + " ; " + configureCmd)
    if not result2:
        raise RuntimeError("Use of 'configure' for IRSTLM has failed.  Please compile IRSTLM"
                           + "manually (see program documentation for details).")
    result3 = system.run("cd " + constants.irstlm_root + "; make ")
    if not result3:
        raise RuntimeError("Use of 'make' for IRSTLM has failed.  Please compile IRSTLM"
                           + "manually (see program documentation for details).")
    result4 = system.run("cd " + constants.irstlm_root + "; make install")
    if not result4:
        raise RuntimeError("Use of 'make install' for IRSTLM has failed.  Please compile IRSTLM"
                           + "manually (see program documentation for details).")


if __name__ == "__main__":
    compile_moses()
    compile_mgizapp()
    compile_irstlm()