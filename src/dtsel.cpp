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


double computePP(ngramtable* train,ngramtable* test,double oovpenalty,double& oovrate){
	
	
	ngram ng2(test->dict);ngram ng1(train->dict);
	int N=0; double H=0;  oovrate=0;
	
	test->scan(ng2,INIT,test->maxlevel());
 	while(test->scan(ng2,CONT,test->maxlevel())) {
		
		ng1.trans(ng2);
		H-=log(prob(train,ng1,ng1.size,0));
		if (*ng1.wordp(1)==train->dict->oovcode()){
			H-=oovpenalty;
			oovrate++;
		}
		N++;
	}
	oovrate/=N;
	return exp(H/N);
}


int main(int argc, char **argv)
{
	char *indom=NULL;   //indomain data: one sentence per line
	char *outdom=NULL;   //domain data: one sentence per line
	char *scorefile=NULL;  //score file 
	int  minfreq=2;    //frequency threshold for dictionary pruning (optional)
	int ngsz=0;        // n-gram size 
	int dub=10000000;  //upper bound of true vocabulary
	int model=0;          //data selection model: 1 only in-domain cross-entropy, 
	//2 cross-entropy difference. 	
	int cv=1;             //cross-validation parameter: 1 only in-domain cross-entropy, 
	char *evalset;        //evalset to measure performance
	int blocksize=100000; //block-size in words
	int verbose=0;
	int index=0; //provided score file includes and index
	double convergence_treshold=0;
	
	DeclareParams((char*)
				  "min-word-freq", CMDINTTYPE, &minfreq,
				  "f", CMDINTTYPE, &minfreq,
				  
				  "ngram-order", CMDSUBRANGETYPE, &ngsz, 1 , MAX_NGRAM,
				  "n", CMDSUBRANGETYPE, &ngsz, 1 , MAX_NGRAM,
				  
				  "in-domain-file", CMDSTRINGTYPE, &indom,
				  "i", CMDSTRINGTYPE, &indom,
				  
				  "out-domain-file", CMDSTRINGTYPE, &outdom,
				  "o", CMDSTRINGTYPE, &outdom,
				  
				  "score-file", CMDSTRINGTYPE, &scorefile,
				  "s", CMDSTRINGTYPE, &scorefile,
				  
				  "dub", CMDINTTYPE, &dub,
				  "dictionary-upper-bound", CMDINTTYPE, &dub,
				  
				  "model", CMDSUBRANGETYPE, &model, 1 , 2,
				  "m", CMDSUBRANGETYPE, &model, 1 , 2,
				  
				  "cross-validation", CMDSUBRANGETYPE, &cv, 1 , 3,
  				  "cv", CMDSUBRANGETYPE, &cv, 1 , 3,
				  
				  "test", CMDSTRINGTYPE, &evalset,
				  "t", CMDSTRINGTYPE, &evalset,
				  
				  "block-size", CMDINTTYPE, &blocksize,
				  "bs", CMDINTTYPE, &blocksize,
				  
				  "convergence-threshold", CMDDOUBLETYPE , &convergence_treshold,
				  "c", CMDDOUBLETYPE , &convergence_treshold,
				  
				  "index", CMDSUBRANGETYPE, &index,0,1,
				  "x", CMDSUBRANGETYPE, &index,0,1,

				  "verbose", CMDSUBRANGETYPE, &verbose,0,2,
				  "v", CMDSUBRANGETYPE, &verbose,0,2,

				  (char *)NULL
				  );
	
	
	
	GetParams(&argc, &argv, (char*) NULL);
	
	
	if (!evalset && (!indom || !outdom)){
		cerr <<"Must specify in-domain and out-domain data files\n";
		exit(1);
	};
	
	//score file is always required: either as output or as input
	if (!scorefile){
		cerr <<"Must specify score file\n";
		exit(1);
	};
	
	if (!evalset && !model){
		cerr <<"Must specify data selection model\n";
		exit(1);
	}
	
	if (evalset && (convergence_treshold<0 || convergence_treshold > 0.1)){
		cerr <<"Convergence threshold must be between 0 and 0.1. \n";
		exit(1);
	}
	
	TABLETYPE table_type=COUNT;
	
	

	
	if (!evalset){
		
		//computed dictionary on indomain data
		dictionary *dict = new dictionary(indom,1000000,0);
		dictionary *pd=new dictionary(dict,true,minfreq);
		delete dict;dict=pd;
		
		int cv; //cross validation
		
		//build in-domain table restricted to the given dictionary
		ngramtable *indngt=new ngramtable(indom,ngsz,NULL,dict,NULL,0,0,NULL,0,table_type);
		double indoovpenalty=-log(dub-indngt->dict->size());
		ngram indng(indngt->dict);
		int indoovcode=indngt->dict->oovcode();
		
		//build out-domain table restricted to the in-domain dictionary
		char command[1000];
			
		if (index)
			sprintf(command,"cut -d \" \" -f 2- %s",outdom);
		else
			sprintf(command,"%s",outdom);
		
		ngramtable *outdngt=new ngramtable(command,ngsz,NULL,dict,NULL,0,0,NULL,0,table_type);
		double outdoovpenalty=-log(dub-outdngt->dict->size());	
		ngram outdng(outdngt->dict);
		int outdoovcode=outdngt->dict->oovcode();
		
		cerr << "dict size idom: " << indngt->dict->size() << " odom: " << outdngt->dict->size() << "\n";
		cerr << "oov penalty idom: " << indoovpenalty << " odom: " << outdoovpenalty << "\n";
		
		//go through the odomain sentences 
		int bos=dict->encode(dict->BoS());int eos=dict->encode(dict->EoS());
		mfstream inp(command,ios::in); ngram ng(dict);
		mfstream txt(outdom,ios::in);
		mfstream output(scorefile,ios::out);
		char line[MAX_LINE];
		
		int lenght=0;float deltaH=0; float deltaHoov=0; int words=0;long index;

		while(inp>>ng){
			
			assert(*ng.wordp(1)==bos);
			
			// reset ngram at begin of sentence
			ng.size=1;
			deltaH=0;deltaHoov=0;	
			lenght=0;
			
			do{
				inp >> ng;
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
			}
			while (*ng.wordp(1) != eos);						
			
			// print score at the end of sentence
			
			txt.getline(line,MAX_LINE);
			output << (deltaH + deltaHoov)/lenght  << " " << line << "\n";													
		}
	}
	else{
		
		//build in-domain evaluation set table 
		ngramtable *tstngt=new ngramtable(evalset,ngsz,NULL,NULL,NULL,0,0,NULL,0,table_type);
		
		//build empty out-domain table
		ngramtable *outdngt=new ngramtable(NULL,ngsz,NULL,NULL,NULL,0,0,NULL,0,table_type);		
		
		dictionary *dict = NULL;
		if (indom){
			cerr << "limit dictionary to indomain frequent words\n";
			//computed dictionary on indomain data
			dict = new dictionary(indom,1000000,0);
			dictionary *pd=new dictionary(dict,true,minfreq);
			delete dict;dict=pd;
			outdngt->dict=dict;
		}
							
		dictionary* outddict=outdngt->dict;
		
		outddict->incflag(1);
		int bos=outddict->encode(outddict->BoS());
		int eos=outddict->encode(outddict->EoS());
		int oov=outddict->encode(outddict->OOV());
		outddict->incflag(0);
		outddict->oovcode(oov);
		
		cerr << "outdict size:" << outdngt->dict->size() << "\n";
		
		double oldPP=dub; double newPP=0; double oovrate=0;  
			
		long totwords=0; long nextstep=blocksize; long totlines=0;
		double score; long index;
		
		mfstream outd(scorefile,ios::in);		
		//initial n-gram	
		ngram ng(outdngt->dict);
		for (int i=1; i<ngsz; i++) ng.pushc(bos);
		ng.freq=1;
		
		if (!dict) outddict->incflag(1);
		while (outd >> score){
			
			if (index) outd >> index;
			
			outd >> ng; assert (*ng.wordp(1) == bos);

			do{
				outd >> ng;
				ng.size=ngsz; //ng.size >= ngsz
				outdngt->put(ng);
				totwords++;
			}
			while (*ng.wordp(1) != eos);
			totlines++;
			
			if (totwords>=nextstep){
				if (!dict) outddict->incflag(0);
				newPP=computePP(outdngt,tstngt,-log(dub-outddict->size()),oovrate);
				if (!dict) outddict->incflag(1);
				
				cout << totwords << " " << newPP;				
				if (verbose) cout << " " << totlines << " " << oovrate;			
				cout << "\n";
				
				if ((newPP-oldPP)/oldPP < convergence_treshold) return 1; 
									
				oldPP=newPP;
				
				nextstep+=blocksize;
			}
		}
		if (!dict) outddict->incflag(0);
		newPP=computePP(outdngt,tstngt,-log(dub-outddict->size()),oovrate);
		cout << totwords << " " << newPP;
		if (verbose) cout << " " << totlines << " " << oovrate;			
		
	}
	
}

	
	
