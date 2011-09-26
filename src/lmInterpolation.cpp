// $Id: lmInterpolation.cpp 3686 2010-10-15 11:55:32Z bertoldi $

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

lmInterpolation::lmInterpolation(float nlf, float dlf){
  ngramcache_load_factor = nlf; 
  dictionary_load_factor = dlf; 
  
  order=0;
  memmap=0;
}

void lmInterpolation::load(const std::string filename,int mmap){
  dictionary_upperbound=1000000;
  int memmap=mmap;


  dict=new dictionary((char *)NULL,1000000,dictionary_load_factor);

  //get info from the configuration file
  fstream inp(filename.c_str(),ios::in|ios::binary);

  char line[MAX_LINE];
  const char* words[MAX_TOKEN_N_MAP];
  int tokenN;
  inp.getline(line,MAX_LINE,'\n');
  tokenN = parseWords(line,words,MAX_TOKEN_N_MAP);

  if (tokenN != 2 || ((strcmp(words[0],"LMINTERPOLATION") != 0) && (strcmp(words[0],"lmmacro")!=0)))
    error((char*)"ERROR: wrong header format of configuration file\ncorrect format: LMINTERPOLATION number_of_models\nweight_of_LM_1 filename_of_LM_1\nweight_of_LM_2 filename_of_LM_2");
  m_number_lm = atoi(words[1]);

  dict->incflag(1);
  for (int i=0;i<m_number_lm;i++){
    inp.getline(line,BUFSIZ,'\n');
    tokenN = parseWords(line,words,3);
    if(tokenN != 2){
      std::cerr << "Wrong input format." << std::endl;
      exit(1);
    }
    m_weight.push_back((float) atof(words[0]));
    m_lm_file.push_back(words[1]);

    m_lm.push_back(load_lm(m_lm_file[i],dictionary_upperbound,memmap,ngramcache_load_factor,dictionary_load_factor));

    dictionary *_dict=m_lm[i]->getDict();
    for (size_t j=0;j<_dict->size();j++){
      dict->encode(_dict->decode(j));
    }
  }
  getDict()->genoovcode();

  dict->incflag(0);
  inp.close();

  int maxorder = 0;
  for (int i=0;i<m_number_lm;i++){
    maxorder = (maxorder > m_lm[i]->maxlevel())?maxorder:m_lm[i]->maxlevel();
  }

  if (order == 0){
    order = maxorder;
    std::cerr << "order is not set; reset to the maximum order of LMs: " << order << std::endl;
  }else if (order > maxorder){
    order = maxorder;
    std::cerr << "order is too high; reset to the maximum order of LMs: " << order << std::endl;
  }
  maxlev=order;
}

lmtable* lmInterpolation::load_lm(std::string file,int dub,int memmap, float nlf, float dlf) {

  //checking the language model type
  int lmtype = getLanguageModelType(file);
  std::cerr << "Language Model Type of " << file << " is " << lmtype << std::endl;
        
  lmtable* lmt = NULL;
  if (lmtype == _IRSTLM_LMMACRO){

    lmt = new lmmacro(nlf,dlf);
		
    //let know that table has inverted n-grams
    //SERVE????      if (invert) lmt->is_inverted(invert);

    lmt->setMaxLoadedLevel(getMaxLoadedLevel());
    ((lmmacro*) lmt)->load(file,memmap);

  }else if (lmtype == _IRSTLM_LMCLASS){
    
    lmt = new lmclass(nlf,dlf);
		
    //let know that table has inverted n-grams
    //SERVE????      if (invert) lmt->is_inverted(invert);

    lmt->setMaxLoadedLevel(getMaxLoadedLevel());
    ((lmclass*) lmt)->load(file,memmap);
  }else if (lmtype == _IRSTLM_LMTABLE){
    lmt=new lmtable(nlf,dlf);

    inputfilestream inplm(file.c_str());
    std::cerr << "Reading " << file << "..." << std::endl;

    lmt->setMaxLoadedLevel(getMaxLoadedLevel());
    if (file.compare(file.size()-3,3,".mm")==0)
       ((lmtable*) lmt)->load(inplm,file.c_str(),NULL,1,NONE);                
    else 
      ((lmtable*) lmt)->load(inplm,file.c_str(),NULL,memmap,NONE);                   
  }else{
    std::cerr << "This language model type is unknown!" << std::endl;
    exit(1);
  }

  //use caches to save time (only if PS_CACHE_ENABLE is defined through compilation flags)
  lmt->init_caches(lmt->maxlevel());
  return lmt;
}

double lmInterpolation::clprob(ngram ng, double* bow,int* bol,char** maxsuffptr,unsigned int* statesize,bool* extendible){

  double pr=0.0;
  double _logpr;

  char* _maxsuffptr=NULL,*actualmaxsuffptr=NULL;
  unsigned int _statesize=0,actualstatesize=0;
  int _bol=0,actualbol=MAX_NGRAM;
  double _bow=0.0,actualbow=0.0;
  bool _extendible=false,actualextendible=false;

  for (size_t i=0;i<m_lm.size();i++){
    
    ngram _ng(m_lm[i]->getDict());
    _ng.trans(ng);
    _logpr=m_lm[i]->clprob(_ng,&_bow,&_bol,&_maxsuffptr,&_statesize,&_extendible);
    //    assert(_statesize != InvalidContextLength);
    
    cerr.precision(10);
    /*
    std::cerr << " LM " << i << " weight:" << m_weight[i] << std::endl;    
    std::cerr << " LM " << i << " log10 logpr:" << _logpr<< std::endl;    
    std::cerr << " LM " << i << " pr:" << pow(10.0,_logpr) << std::endl;    
    std::cerr << " _statesize:" << _statesize << std::endl;
    std::cerr << " _bow:" << _bow << std::endl;
    std::cerr << " _bol:" << _bol << std::endl;
    */

    //TO CHECK the following claims
    //What is the statesize of a LM interpolation? The largest _statesize among the submodels
    //What is the maxsuffptr of a LM interpolation? The _maxsuffptr of the submodel with the largest _statesize
    //What is the bol of a LM interpolation? The smallest _bol among the submodels
    //What is the bow of a LM interpolation? The weighted sum of the bow of the submodels
    //What is the prob of a LM interpolation? The weighted sum of the prob of the submodels
    //What is the extendible flag of a LM interpolation? true if the extendible flag is one for any LM
    
    pr+=m_weight[i]*pow(10.0,_logpr);
    actualbow+=m_weight[i]*pow(10.0,_bow);

    if(_statesize > actualstatesize || i == 0) {
      actualmaxsuffptr = _maxsuffptr;
      actualstatesize = _statesize;
    }
    if (_bol < actualbol){
      actualbol=_bol; //backoff limit of LM[i] 
    }
    if (_extendible){
      actualextendible=true; //set extendible flag to true if the ngram is extendible for any LM
    }
      
    if (bol) *bol=actualbol;
    if (bow) *bow=log(actualbow);
    if (maxsuffptr) *maxsuffptr=actualmaxsuffptr;
    if (statesize) *statesize=actualstatesize;
    if (extendible) *extendible=actualextendible;
  }
  /*
  if (statesize) std::cerr << " statesize:" << *statesize << std::endl;
  if (bow) std::cerr << " bow:" << *bow << std::endl;
  if (bol) std::cerr << " bol:" << *bol << std::endl;
  */
  return log(pr)/M_LN10;
}
 
double lmInterpolation::clprob(int* codes, int sz, double* bow,int* bol,char** maxsuffptr,unsigned int* statesize,bool* extendible){

  //create the actual ngram
  ngram ong(dict);
  ong.pushc(codes,sz);
  assert (ong.size == sz);

  return clprob(ong, bow, bol, maxsuffptr, statesize, extendible);
} 
