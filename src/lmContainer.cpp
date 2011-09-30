// $Id: lmContainer.cpp 3686 2010-10-15 11:55:32Z bertoldi $

/******************************************************************************
IrstLM: IRST Language Model Toolkit
Copyright (C) 2006 Marcello Federico, ITC-irst Trento, Italy

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA

******************************************************************************/
#include <stdio.h>
#include <cstdlib>
#include <stdlib.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <cassert>
#include "lmContainer.h"
#include "lmtable.h"
#include "lmmacro.h"
#include "lmclass.h"
#include "lmInterpolation.h"

using namespace std;

inline void error(const char* message){
  std::cerr << message << "\n";
  throw std::runtime_error(message);
}

lmContainer::lmContainer(){ requiredMaxlev=1000; }

int lmContainer::getLanguageModelType(std::string filename){
  fstream inp(filename.c_str(),ios::in|ios::binary);

  if (!inp.good()) {
    std::cerr << "Failed to open " << filename << "!" << std::endl;
    exit(1);
  }
  //give a look at the header to get informed about the language model type
  std::string header;
  inp >> header;
  inp.close();

  VERBOSE(1,"LM header:|" << header << "|" << std::endl);
 
  int type=_IRSTLM_LMUNKNOWN;
  VERBOSE(1,"type: " << type << std::endl);
  if (header == "lmminterpolation" || header == "LMINTERPOLATION"){
        type = _IRSTLM_LMINTERPOLATION;
  }else if (header == "lmmacro" || header == "LMMACRO"){
        type = _IRSTLM_LMMACRO;
  }else if (header == "lmclass" || header == "LMCLASS"){
        type = _IRSTLM_LMCLASS;
  }else{
        type = _IRSTLM_LMTABLE;
  }
  VERBOSE(1,"type: " << type << std::endl);

  return type;
};

lmContainer* lmContainer::CreateLanguageModel(const std::string infile, float nlf, float dlf){

  int type = getLanguageModelType(infile);
  std::cerr << "Language Model Type of " << infile << " is " << type << std::endl;

  lmContainer* lm=NULL;

  switch (type){

  case _IRSTLM_LMTABLE:
    lm = new lmtable(nlf, dlf);
    break;

  case _IRSTLM_LMMACRO:
    lm = new lmmacro(nlf, dlf);
    break;

  case _IRSTLM_LMCLASS:
    lm = new lmclass(nlf, dlf);
    break;

  case _IRSTLM_LMINTERPOLATION:
    lm = new lmInterpolation(nlf, dlf);
    break;

  }

  if (lm == NULL) {
    std::cerr << "This language model type is unknown!" << std::endl;
    exit(1);
  }

  lm->setLanguageModelType(type);
  return lm;
};

