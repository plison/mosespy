// $Id: ngt.cpp 245 2009-04-02 14:05:40Z fabio_brugnara $

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

// dtsel
// by M. Federico
// Copyright Marcello Federico, ITC-irst, 2012

using namespace std;

#include <cmath>
#include "util.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "dictionary.h"
#include "n_gram.h"
#include "ngramtable.h"
#include "cmd.h"

#define YES   1
#define NO    0

#define END_ENUM    {   (char*)0,  0 }

static Enum_T BooleanEnum [] = {
  {    (char*)"Yes",    YES },
  {    (char*)"No",     NO},
  {    (char*)"yes",    YES },
  {    (char*)"no",     NO},
  {    (char*)"y",    YES },
  {    (char*)"n",     NO},
  END_ENUM
};


double prob(ngramtable* ngt,ngram ng,int size,int cv){
	double fstar,lambda;
	
	assert(size<=ngt->maxlevel() && size<=ng.size);	
	if (size>1){				
		ngram history=ng;
		if (ngt->get(history,size,size-1) && history.freq>cv){
			fstar=0.0;
			if (ngt->get(ng,size,size)){
				cv=(cv>ng.freq)?ng.freq:cv;
				if (ng.freq>cv){
					fstar=(double)(ng.freq-cv)/(double)(history.freq -cv + history.succ);					
					lambda=(double)history.succ/(double)(history.freq -cv + history.succ);
				}else //ng.freq==cv
					lambda=(double)(history.succ-1)/(double)(history.freq -cv + history.succ-1);
			}
			else
				lambda=(double)history.succ/(double)(history.freq -cv + history.succ);			
			
			return fstar + lambda * prob(ngt,ng,size-1,cv);
		}
		else return prob(ngt,ng,size-1,cv);
		
	}else{ //unigram branch
		if (ngt->get(ng,1,1) && ng.freq>cv)
			return (double)(ng.freq-cv)/(ngt->totfreq()-1);
		else{
			//cerr << "backoff to oov unigram " << ng.freq << " " << cv << "\n";
			*ng.wordp(1)=ngt->dict->oovcode();
			if (ngt->get(ng,1,1) && ng.freq>0)
				return (double)ng.freq/ngt->totfreq();
			else //use an automatic estimate of Pr(oov)
				return (double)ngt->dict->size()/(ngt->totfreq()+ngt->dict->size());				
		}

	}
	
}



int main(int argc, char **argv)
{
	char *indom=NULL;   //indomain data: one sentence per line
	char *outdom=NULL;   //domain data: one sentence per line
	char *outfile=NULL;  //output file 
	int  minfreq=2;    //frequency threshold for dictionary pruning (optional)
	int ngsz=0;        // n-gram size 
	int dub=10000000;  //upper bound of true vocabulary
	char *out=NULL;    //output file with scores
	int model=0;       //data selection model: 1 only in-domain cross-entropy, 
	//2 cross-entropy difference. 	
	int cv=1;       //cross-validation parameter: 1 only in-domain cross-entropy, 
	
	DeclareParams((char*)
				  "min-word-freq", CMDINTTYPE, &minfreq,
				  "f", CMDINTTYPE, &minfreq,
				  
				  "ngram-order", CMDSUBRANGETYPE, &ngsz, 1 , MAX_NGRAM,
				  "n", CMDSUBRANGETYPE, &ngsz, 1 , MAX_NGRAM,
				  
				  "in-domain-file", CMDSTRINGTYPE, &indom,
				  "i", CMDSTRINGTYPE, &indom,
				  
				  "out-domain-file", CMDSTRINGTYPE, &outdom,
				  "o", CMDSTRINGTYPE, &outdom,
				  
				  "score-file", CMDSTRINGTYPE, &outfile,
				  "s", CMDSTRINGTYPE, &outfile,
				  
				  "dub", CMDINTTYPE, &dub,
				  "dictionary-upper-bound", CMDINTTYPE, &dub,
				  
				  "model", CMDSUBRANGETYPE, &model, 1 , 2,
				  "m", CMDSUBRANGETYPE, &model, 1 , 2,
				  
				  "cv", CMDSUBRANGETYPE, &cv, 1 , 3,
				  
				  (char *)NULL
				  );
	
	
	
	GetParams(&argc, &argv, (char*) NULL);
	
	if (indom==NULL || outdom==NULL){
		cerr <<"Must specify in-domain and out-domain data files\n";
		exit(1);
	};
	
	if (outfile==NULL){
		cerr <<"Must specify output file\n";
		exit(1);
	};
	
	if (!model){
		cerr <<"Must specify data selection model\n";
		exit(1);
	}
	
	TABLETYPE table_type=COUNT;
	int cv; //cross validation
	
	//computed dictionary on indomain data
	dictionary *dict = new dictionary(indom,1000000,0);
	dictionary *pd=new dictionary(dict,true,minfreq);
	delete dict;dict=pd;
	
	//build in-domain table restricted to the given dictionary
	ngramtable *indngt=new ngramtable(indom,ngsz,NULL,dict,NULL,0,0,NULL,0,table_type);
	double indoovpenalty=-log(dub-indngt->dict->size());
	ngram indng(indngt->dict);
	int indoovcode=indngt->dict->oovcode();
	
	//build out-domain table restricted to the in-domain dictionary
	ngramtable *outdngt=new ngramtable(outdom,ngsz,NULL,dict,NULL,0,0,NULL,0,table_type);
	double outdoovpenalty=-log(dub-outdngt->dict->size());	
	ngram outdng(outdngt->dict);
	int outdoovcode=outdngt->dict->oovcode();
	
	cerr << "dict size idom: " << indngt->dict->size() << " odom: " << outdngt->dict->size() << "\n";
	cerr << "oov penalty idom: " << indoovpenalty << " odom: " << outdoovpenalty << "\n";
	
	//go through the odomain sentences 
	int bos=dict->encode(dict->BoS());int eos=dict->encode(dict->EoS());
	mfstream inp(outdom,ios::in); ngram ng(dict);
	mfstream txt(outdom,ios::in);
	mfstream output(outfile,ios::out);
	char line[MAX_LINE];
	
    int lenght=0;float deltaH=0; float deltaHoov=0; int words=0;
	while(inp >> ng){
		// reset ngram at begin of sentence
		if (*ng.wordp(1)==bos){
			ng.size=1;
			deltaH=0;deltaHoov=0;	
			lenght=0;
			continue;
		}

		
		lenght++; words++;

		if ((words % 1000000)==0) cerr << ".";
		
		
		if (ng.size>ngsz) ng.size=ngsz;
		indng.trans(ng);outdng.trans(ng);
		
		if (model==1){
			deltaH-=log(prob(indngt,indng,indng.size,cv=0));	
			deltaHoov-=(*indng.wordp(1)==indoovcode?indoovpenalty:0);
		}
		if (model==2){
			deltaH+=log(prob(outdngt,outdng,outdng.size,cv=2))-log(prob(indngt,indng,indng.size,cv=0));	
			deltaHoov+=(*outdng.wordp(1)==outdoovcode?outdoovpenalty:0)-(*indng.wordp(1)==indoovcode?indoovpenalty:0);
		}
		
		if (*ng.wordp(1)==eos){
			txt.getline(line,MAX_LINE);
			output << (deltaH + deltaHoov)/lenght  << " " << line << "\n";			
		}				
		
	}
	
}

	
	
	
