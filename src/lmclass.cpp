// $Id: lmclass.cpp 3631 2010-10-07 12:04:12Z bertoldi $

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
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <cassert>
#include "math.h"
#include "mempool.h"
#include "htable.h"
#include "ngramcache.h"
#include "dictionary.h"
#include "n_gram.h"
#include "lmtable.h"
#include "lmclass.h"
#include "util.h"

using namespace std;

// local utilities: start

int parseWords(char *sentence, const char **words, int max);

inline void error(const char* message){
  cerr << message << "\n";
  throw runtime_error(message);
}

// local utilities: end



lmclass::lmclass(float nlf, float dlfi):lmtable(nlf,dlfi){
  dict = new dictionary((char *)NULL,1000000); //word to cluster dictionary
  W2Clprb= (double *)malloc(1000000*sizeof(double));// //array of probabilities
  memset(W2Clprb,0,1000000*sizeof(double));
};

lmclass::~lmclass(){
  free (W2Clprb);
}

void lmclass::load(const std::string filename,int memmap){

  //get info from the configuration file
  fstream inp(filename.c_str(),ios::in|ios::binary);

  char line[MAX_LINE];
  const char* words[MAX_TOKEN];
  int tokenN;
  inp.getline(line,MAX_LINE,'\n');
  tokenN = parseWords(line,words,MAX_TOKEN);

  if (tokenN != 2 || ((strcmp(words[0],"LMCLASS") != 0) && (strcmp(words[0],"lmclass")!=0)))
    error((char*)"ERROR: wrong header format of configuration file\ncorrect format: LMCLASS LM_order\nfilename_of_LM\nfilename_of_map");

  maxlev = atoi(words[1]);
  std::string lmfilename;
  if (inp.getline(line,MAX_LINE,'\n')){
    tokenN = parseWords(line,words,MAX_TOKEN);
    lmfilename = words[0];
  }else{
    error((char*)"ERROR: wrong header format of configuration file\ncorrect format: LMCLASS LM_order\nfilename_of_LM\nfilename_of_map");
  }

  std::string W2Cdict = "";
  if (inp.getline(line,MAX_LINE,'\n')){
    tokenN = parseWords(line,words,MAX_TOKEN);
    W2Cdict = words[0];
  }else{
    error((char*)"ERROR: wrong header format of configuration file\ncorrect format: LMCLASS LM_order\nfilename_of_LM\nfilename_of_map");
  }
  inp.close();

  std::cerr << "lmfilename:" << lmfilename << std::endl;
  if (W2Cdict != ""){
    std::cerr << "mapfilename:" << W2Cdict << std::endl;
  }else{
    error((char*)"ERROR: you must specify a map!");
  }


  // Load the (possibly binary) LM 
  inputfilestream inpLM(lmfilename.c_str());
  if (!inpLM.good()) {
    std::cerr << "Failed to open " << lmfilename << "!" << std::endl;
    exit(1);
  }
  lmtable::load(inpLM,lmfilename.c_str(),NULL,memmap);

  inputfilestream inW2C(W2Cdict);
  if (!inW2C.good()) {
    std::cerr << "Failed to open " << lmfilename << "!" << std::endl;
    exit(1);
  }
  loadW2Cdict(inW2C);

  cerr << "OOV code of lmclass is " << getDict()->oovcode() << "\n";
  cerr << "OOV code of lmtable is " << lmtable::getDict()->oovcode() << "\n";

}

void lmclass::loadW2Cdict(istream& inW2C){
  
  double lprob=0.0;
  int howmany=0,wcode=0;
  const char* words[1 + LMTMAXLEV + 1 + 1];
  //open input stream and prepare an input string
  char line[MAX_LINE];
  //dictionary(NULL,1000000); //??????which dictionary??
  dict->incflag(1); //can add to the map dictionary
  dict->incflag(1); //can add to the dictionary of lmclass

  cerr<<"loadW2Cdict()...\n";
  //save freq of EOS and BOS
  wcode=dict->encode(dictW2C->BoS());
  dict->freq(wcode,lmtable::dict->encode(dict->BoS()));
  W2Clprb[wcode]=0.0;
  wcode=dict->encode(dictW2C->EoS());
  dict->freq(wcode,lmtable::dict->encode(dict->EoS()));
  W2Clprb[wcode]=0.0;
  //should i add <unk> to the dict or just let the trans_freq handle <unk>
  wcode=dict->encode(dictW2C->OOV());
  dict->freq(wcode,lmtable::dict->encode(dict->OOV()));
  W2Clprb[wcode]=0.0;

  while (inW2C.getline(line,MAX_LINE)){
    if (strlen(line)==MAX_LINE-1){
      cerr << "lmtable::loadW2Cdict: input line exceed MAXLINE ("
	   << MAX_LINE << ") chars " << line << "\n";
      exit(1);
    }

    howmany = parseWords(line, words, 4); //3

    if(howmany == 3){
      assert(sscanf(words[2], "%lf", &lprob));
      lprob=(double)log10(lprob);
    }
    else{
      if(howmany==2)
	lprob=0.0;
      else{
	cerr << "parseline: not enough entries" << line << "\n";
	exit(1);
      }
    }
    //freq of word(words[0]) will be encoding of clusterID (words[1])
    wcode=dict->encode(words[0]);
    dict->freq(wcode,lmtable::dict->encode(words[1]));
    //save the probability associated with the words -> index by word encoding
    W2Clprb[wcode]=lprob;  
  }

  dict->incflag(0); //can NOT add to the dictionary of lmclass
  dict->genoovcode();
}

double lmclass::lprob(ngram ong,double* bow, int* bol, char** maxsuffptr,unsigned int* statesize,bool* extendible){
  double lpr=W2Clprb[*ong.wordp(1)];

  //convert ong to it's clustered encoding
  ngram ngt(lmtable::getDict());
  ngt.trans_freq(ong);

  lpr+=lmtable::clprob(ngt,bow,bol,maxsuffptr,statesize, extendible);
  return lpr;
}
