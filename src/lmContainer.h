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

class lmContainer
{
  static const bool debug=true;

protected:
  int          lmtype; //auto reference to its own type
  int          maxlev; //maximun order of sub LMs;
  int  requiredMaxlev; //max loaded level, i.e. load up to requiredMaxlev levels

public:

  lmContainer();
  virtual ~lmContainer() {};

	 
  virtual void load(const std::string &filename, int mmap=0) {
    UNUSED(filename);
    UNUSED(mmap);
  };
	
	virtual void savetxt(const char *filename) {
    UNUSED(filename);
  };
  virtual void savebin(const char *filename) {
    UNUSED(filename);
  };

  virtual double getlogOOVpenalty() const {
    return 0.0;
  };
  virtual double setlogOOVpenalty(int dub) {
    UNUSED(dub);
    return 0.0;
  };
  virtual double setlogOOVpenalty(double oovp) {
    UNUSED(oovp);
    return 0.0;
  };

  inline virtual dictionary* getDict() const {
    return NULL;
  };
  inline virtual void maxlevel(int lev) {
    maxlev = lev;
  };
  inline virtual int maxlevel() const {
    return maxlev;
  };
  inline virtual void stat(int lev=0) {
    UNUSED(lev);
  };

  inline virtual void setMaxLoadedLevel(int lev) {
    requiredMaxlev=lev;
  };
  inline virtual int getMaxLoadedLevel() {
    return requiredMaxlev;
  };

  virtual bool is_inverted(const bool flag) {
    UNUSED(flag);
    return false;
  };
  virtual bool is_inverted() {
    return false;
  };
  virtual double clprob(ngram ng, double* bow=NULL, int* bol=NULL, char** maxsuffptr=NULL, unsigned int* statesize=NULL,bool* extendible=NULL) {
    UNUSED(ng);
    UNUSED(bow);
    UNUSED(bol);
    UNUSED(maxsuffptr);
    UNUSED(statesize);
    UNUSED(extendible);
    return 0.0;
  };
  virtual double clprob(int* ng, int ngsize, double* bow=NULL, int* bol=NULL, char** maxsuffptr=NULL, unsigned int* statesize=NULL,bool* extendible=NULL) {
    UNUSED(ng);
    UNUSED(ngsize);
    UNUSED(bow);
    UNUSED(bol);
    UNUSED(maxsuffptr);
    UNUSED(statesize);
    UNUSED(extendible);
    return 0.0;
  };

  virtual void used_caches() {};
  virtual void init_caches(int uptolev) {
    UNUSED(uptolev);
  };
  virtual void check_caches_levels() {};
  virtual void reset_caches() {};

  virtual void  reset_mmap() {};

  void inline setLanguageModelType(int type) {
    lmtype=type;
  };
  int getLanguageModelType() const {
    return lmtype;
  };
  static int getLanguageModelType(std::string filename);

  inline virtual void dictionary_incflag(const bool flag) {
    UNUSED(flag);
  };

  virtual bool filter(const string sfilter, lmContainer*& sublmt, const string skeepunigrams);

  static lmContainer* CreateLanguageModel(const std::string infile, float nlf=0.0, float dlf=0.0);
  static lmContainer* CreateLanguageModel(int type, float nlf=0.0, float dlf=0.0);

  inline virtual bool is_OOV(int code) {
    UNUSED(code);
    return false;
  };


  inline static bool is_lmt_cache_enabled(){
    #ifdef LMT_CACHE_ENABLE
      return true;
    #else
      return false;
    #endif
  }

  inline static bool is_ps_cache_enabled(){
    #ifdef PS_CACHE_ENABLE
		return true;
		#else
		  return false;
    #endif
  }

  inline static bool is_cache_enabled(){
    return is_lmt_cache_enabled() && is_ps_cache_enabled();
  }
};


#endif

