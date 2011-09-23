// $Id: lmInterpolation.h 3686 2010-10-15 11:55:32Z bertoldi $

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

#ifndef MF_LMINTERPOLATION_H
#define MF_LMINTERPOLATION_H

#include <stdio.h>
#include <cstdlib>
#include <stdlib.h>
#include <string>
#include <vector>
#include "dictionary.h"
#include "n_gram.h"
#include "lmContainer.h"


/*
interpolation of lmtable and lmmacro and lmclass (NOT lmInterpolation)
*/


class lmInterpolation: public lmContainer{
  static const bool debug=true;
  int m_number_lm;
  int order;
  int dictionary_upperbound; //set by user
  double  logOOVpenalty; //penalty for OOV words (default 0)

  int memmap;  //level from which n-grams are accessed via mmap

  std::vector<double> m_weight;
  std::vector<std::string> m_lm_file;
  std::vector<lmContainer*> m_lm;

  int               maxlev; //maximun order of sub LMs;

  float ngramcache_load_factor;
  float dictionary_load_factor;

  dictionary *dict; // dictionary for all interpolated LMs

 public:
    
  lmInterpolation(float nlf=0.0, float dlfi=0.0);
  virtual ~lmInterpolation(){};

  void load(const std::string filename,int mmap=0);
  lmtable* load_lm(std::string file,int dub,int memmap, float nlf, float dlf);

  virtual double clprob(ngram ng,            double* bow=NULL,int* bol=NULL,char** maxsuffptr=NULL,unsigned int* statesize=NULL,bool* extendible=NULL); 
  virtual double clprob(int* ng, int ngsize, double* bow=NULL,int* bol=NULL,char** maxsuffptr=NULL,unsigned int* statesize=NULL,bool* extendible=NULL); 

  int maxlevel() const {return maxlev;};

  virtual inline void setDict(dictionary* d) { dict=d; };
  virtual inline dictionary* getDict() const { return dict; };

  //set penalty for OOV words  
  virtual double getlogOOVpenalty() const { return logOOVpenalty; }
  
  virtual double setlogOOVpenalty(int dub){ 
    assert(dub > dict->size());
    double _logpr;
    double OOVpenalty=0.0;
    for (int i=0;i<m_number_lm;i++){
      m_lm[i]->setlogOOVpenalty(dub);  //set OOV Penalty for each LM
      _logpr=m_lm[i]->getlogOOVpenalty();
      OOVpenalty+=m_weight[i]*exp(_logpr);
    }
    logOOVpenalty=log(OOVpenalty);
    return logOOVpenalty;
  }
  
  double setlogOOVpenalty2(double oovp){ 
    return logOOVpenalty=oovp;
  }

  /*
  inline virtual void setMaxLoadedLevel(int lev){
    lmContainer::setMaxLoadedLevel(lev);
    for (int i=0;i<m_number_lm;i++){
      m_lm[i]->setMaxLoadedLevel(lev);  //set the vaalue for each LM
    }
  };
*/
};

#endif

