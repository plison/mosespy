AC_INIT([irstlm], [5.80.06])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

AC_CHECK_TOOL(PDFLATEX,pdflatex,"no")
AC_CHECK_TOOL(BIBTEX,bibtex,"no")

AC_ARG_ENABLE([doc],
    AS_HELP_STRING([--enable-doc], [enable creation of documentation]))

AM_CONDITIONAL([DOC_COMPILATION],false)
if test "x$enable_doc" = 'xyes'
then
    AM_CONDITIONAL([DOC_COMPILATION],true)
    if test "x$PDFLATEX" != "xno" && test "x$BIBTEX" != "xno"
    then
        AC_MSG_NOTICE([pdflatex is available])
        AC_MSG_NOTICE([documentation will be created])
    else
        AM_CONDITIONAL([DOC_COMPILATION],false)
        AC_MSG_NOTICE([either pdflatex or bibtex is not available])
        AC_MSG_NOTICE([documentation will not be created (default); get it through the website $WEBSITE])
    fi
else
    AC_MSG_NOTICE([documentation will not be created (default); get it through the website $WEBSITE])
fi


AC_CONFIG_FILES([
Makefile
src/Makefile
scripts/Makefile
doc/Makefile
])

AC_SUBST(transform,'s/_lm/-lm/')

AC_OUTPUT()

AC_MSG_NOTICE([The software will be installed into $prefix])
