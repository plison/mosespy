# include specific modules
INCLUDE(FindLATEX)
INCLUDE(UseLATEX)

if (PDFLATEX_COMPILER)
message("PDFLATEX_COMPILER exists (${PDFLATEX_COMPILER})")

SET(LATEX_OUTPUT_PATH build)

PROJECT(irstlm-manual NONE)
cmake_minimum_required(VERSION 2.8)

SET(IRSTLM_INPUT_TEX ClassAndChunkLMs.tex LMFileFormats.tex LMFiltering.tex LMInterface.tex LMInterpolation.tex LMPruning.tex LMQuantization.tex LMadaptation.tex LMcompilation.tex LMprune.tex LMsmoothing.tex compileLM.tex dict.tex gettingStarted.tex giganticLM.tex installation.tex interpolateLM.tex interpolatedLM.tex introduction.tex mixtureLM.tex ngt.tex parallelComputation.tex pruneLM.tex quantizeLM.tex referenceMaterial.tex regressionTests.tex releaseNotes.tex tlm.tex )

ADD_LATEX_DOCUMENT(
    ./irstlm-manual.tex
    INPUTS ${IRSTLM_INPUT_TEX}
    DEFAULT_PDF
)

add_custom_command(TARGET pdf POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
       ${CMAKE_CURRENT_BINARY_DIR}/${LATEX_OUTPUT_PATH}/irstlm-manual.pdf
       ${CMAKE_BINARY_DIR}/doc/irstlm-${IRSTLM_VERSION}-manual.pdf)

INSTALL(PROGRAMS irstlm-${IRSTLM_VERSION}-manual.pdf
    DESTINATION doc
    PERMISSIONS OWNER_READ
    )

ELSE()
message("PDFLATEX_COMPILER does not exists")
ENDIF()

