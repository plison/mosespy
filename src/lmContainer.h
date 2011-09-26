// $Id: lmContainer.h 3686 2010-10-15 11:55:32Z bertoldi $

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

#ifndef MF_LMCONTAINER_H
#define MF_LMCONTAINER_H

#define _IRSTLM_LMUNKNOWN 0
#define _IRSTLM_LMTABLE 1
#define _IRSTLM_LMMACRO 2
#define _IRSTLM_LMCLASS 3
#define _IRSTLM_LMINTERPOLATION 4


#include <stdio.h>
#include <cstdlib>
#include <stdlib.h>
#include "util.h"
#include "n_gram.h"
#include "dictionary.h"

typedef enum {BINARY,TEXT,YRANIB,NONE} OUTFILE_TYPE;

class lmContainer{
  static const bool debug=true;

 protected:
  int          maxlev; //maximun order of sub LMs;
  int  requiredMaxlev; //max loaded level, i.e. load up to requiredMaxlev levels

 public:
    
  lmContainer();
  virtual ~lmContainer(){};

  virtual void load(std::istream& inp,const char* filename=NULL,const char* outfilename=NULL,int mmap=0,OUTFILE_TYPE outtype=NONE){};

  virtual void savetxt(const char */*filename*/){};
  virtual void savebin(const char */*filename*/){};
  virtual double getlogOOVpenalty() const{return 0.0;};
  virtual double setlogOOVpenalty(int /*dub*/){return 0.0;};
  virtual double setlogOOVpenalty2(double /*oovp*/){return 0.0;};
  virtual inline dictionary* getDict() const{ return NULL;};
  virtual int maxlevel() const{ return 0;};
  virtual void stat(int /*lev=0*/){};
  virtual void stat(){};

  inline virtual void setMaxLoadedLevel(int lev){ requiredMaxlev=lev; };
  inline virtual int getMaxLoadedLevel(){ return requiredMaxlev; };

  virtual bool is_inverted(const bool /*flag*/){return true;};
  virtual bool is_inverted(){return true;};
  virtual double clprob(ngram ng, double* bow=NULL, int* bol=NULL, char** maxsuffptr=NULL, unsigned int* statesize=NULL,bool* extendible=NULL){
    std::cerr << "lmInterpolation::clprob(ngram ng,...)" << std::endl;
 return 0.0;
  };
  virtual double clprob(int* ng, int ngsize, double* bow=NULL, int* bol=NULL, char** maxsuffptr=NULL, unsigned int* statesize=NULL,bool* extendible=NULL){
    std::cerr << "lmInterpolation::clprob(int* ng, int ngsize,...)" << std::endl;
    return 0.0;
  }
//  virtual double clprob(ngram /*ng*/, double* /*bow*/,int* /*bol*/,char** /*maxsuffptr*/,unsigned int* /*statesize*/){return 0.0;};
//  virtual double clprob(ngram ng, double* bow,int* bol){    std::cerr << "lmContainer::clprob(ngram ng, double* bow,int* bol) START" << std::endl; return clprob(ng,bow,bol,NULL,NULL);}; 
//  virtual double clprob(ngram ng){std::cerr << "lmContainer::clprob(ngram ng) START" << std::endl;return clprob(ng,NULL,NULL,NULL,NULL);}; 

  virtual void used_caches(){};
  virtual void init_caches(int /*uptolev*/){};
  virtual void check_caches_levels(){};
  virtual void reset_caches(){};
 
  virtual void  reset_mmap(){};
};

inline int getLanguageModelType(std::string filename){
  fstream inp(filename.c_str(),ios::in|ios::binary);

  if (!inp.good()) {
    std::cerr << "Failed to open " << filename << "!" << std::endl;
    exit(1);
  }
  //give a look at the header to get informed about the language model type
  std::string header;
  inp >> header;
  inp.close();

  if (header == "lmminterpolation" || header == "LMINTERPOLATION"){
        return _IRSTLM_LMINTERPOLATION;
  }else if (header == "lmmacro" || header == "LMMACRO"){
	return _IRSTLM_LMMACRO;
  }else if (header == "lmclass" || header == "LMCLASS"){
	return _IRSTLM_LMCLASS;
  }else{
	return _IRSTLM_LMTABLE;
  }

  return _IRSTLM_LMUNKNOWN;
}

#endif

