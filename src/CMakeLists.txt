# Set output directory
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

SET(GCC_COVERAGE_LINK_FLAGS  "-lz -lm" )
SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}" )

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isystem/usr/include -W -Wall -ffor-scope")

ADD_DEFINITIONS("-D_LARGE_FILES")
ADD_DEFINITIONS("-D_FILE_OFFSET_BITS=64")
ADD_DEFINITIONS("-DMYCODESIZE=3")
ADD_DEFINITIONS("-DDEBUG")

IF (WIN32)

ELSE()
ADD_DEFINITIONS("-Wno-deprecated")
ADD_DEFINITIONS("-Wno-write-strings")
ENDIF()

INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/src")

SET( LIB_IRSTLM_SRC
        cmd.h cmd.c
        gzfilebuf.h index.h
        dictionary.h dictionary.cpp
        htable.h htable.cpp 
        lmContainer.h lmContainer.cpp 
        lmclass.h lmclass.cpp 
        lmmacro.h lmmacro.cpp
        lmtable.h lmtable.cpp
        lmInterpolation.h lmInterpolation.cpp
        mempool.h mempool.cpp 
        mfstream.h mfstream.cpp 
        n_gram.h n_gram.cpp 
        ngramcache.h ngramcache.cpp
        ngramtable.h ngramtable.cpp
        timer.h timer.cpp 
        util.h util.cpp 
        crc.h crc.cpp 
	interplm.h interplm.cpp
        linearlm.h linearlm.cpp
        mdiadapt.h mdiadapt.cpp 
        mixture.h mixture.cpp 
        normcache.h normcache.cpp 
        shiftlm.h shiftlm.cpp
        cplsa.h cplsa.cpp 
        doc.h doc.cpp
)

########

IF (WIN32)
        INCLUDE_DIRECTORIES( ${PROJECT_SOURCE_DIR}/w32 )
        LINK_DIRECTORIES ( ${PROJECT_SOURCE_DIR}/w32 )
        IF( USE_64_BIT )
                SET(CMAKE_THREAD_LIBS_INIT pthread64)
        ELSE( USE_64_BIT )
                SET(CMAKE_THREAD_LIBS_INIT pthread)
        ENDIF(  )
ENDIF() 

INCLUDE_DIRECTORIES( ${PROJECT_SOURCE_DIR}/src/ )
LINK_DIRECTORIES ( ${LIBRARY_OUTPUT_PATH} )

ADD_LIBRARY(irstlm STATIC ${LIB_IRSTLM_SRC})
SET_TARGET_PROPERTIES(irstlm PROPERTIES OUTPUT_NAME "irstlm")
LINK_DIRECTORIES (${LIBRARY_OUTPUT_PATH})

INSTALL(TARGETS irstlm LIBRARY DESTINATION lib ARCHIVE DESTINATION lib )

FOREACH(CMD compile-lm dict dtsel interpolate-lm ngt plsa prune-lm quantize-lm score-lm tlm verify-caching)

ADD_EXECUTABLE(${CMD} ${CMD}.cpp)
TARGET_LINK_LIBRARIES (${CMD} irstlm)

INSTALL(TARGETS ${CMD} RUNTIME DESTINATION bin )

ENDFOREACH()



