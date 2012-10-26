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

using namespace std;

#include <cmath>
#include "mfstream.h"
#include "mempool.h"
#include "dictionary.h"
#include "n_gram.h"
#include "ngramtable.h"
#include "interplm.h"
#include "normcache.h"
#include "ngramcache.h"
#include "mdiadapt.h"
#include "shiftlm.h"
#include "linearlm.h"
#include "mixture.h"
#include "cmd.h"
#include "util.h"

//
//Mixture interpolated language model
//

static Enum_T SLmTypeEnum [] = {
  {    (char*)"ModifiedShiftBeta",  MOD_SHIFT_BETA },
  {    (char*)"msb",                MOD_SHIFT_BETA },
  {    (char*)"InterpShiftBeta",    SHIFT_BETA },
  {    (char*)"sb",                 SHIFT_BETA },
  {    (char*)"InterpShiftOne",     SHIFT_ONE },
  {    (char*)"s1",                 SHIFT_ONE },
  {    (char*)"InterpShiftZero",    SHIFT_ZERO },
  {    (char*)"s0",                 SHIFT_ZERO },
  {    (char*)"LinearWittenBell",   LINEAR_WB },
  {    (char*)"wb",                 LINEAR_WB },
  {    (char*)"Mixture",            MIXTURE},
  END_ENUM
};


mixture::mixture(bool fulltable,char* sublminfo,int depth,int prunefreq,char* ipfile,char* opfile):
  mdiadaptlm((char *)NULL,depth)
{

  prunethresh=prunefreq;
  ipfname=ipfile;
  opfname=opfile;
  usefulltable=fulltable;

  mfstream inp(sublminfo,ios::in );
  if (!inp) {
    cerr << "cannot open " << sublminfo << "\n";
    exit(1);
  }

  inp >> numslm;

  sublm=new interplm* [numslm];
  int slmtype;
  bool subprunesingletons;
  bool subprunetopsingletons;
  int subprunefreq;

  char *subtrainfile;

  DeclareParams((char*)
    "slm",CMDENUMTYPE, &slmtype, SLmTypeEnum,
    "str",CMDSTRINGTYPE, &subtrainfile,
    "sp",CMDSUBRANGETYPE, &subprunefreq, 0 , 1000,
    "sps",CMDBOOLTYPE, &subprunesingletons,
    "spts",CMDBOOLTYPE, &subprunetopsingletons,
    (char *)NULL
  );

  cerr << "WARNING: Parameters PruneTopSingletons (ps) and PruneSingletons (pts) are not taken into account for this type of LM (mixture); please specify the singleton pruning policy for each submodel using parameters \"-sps\" and \"-spts\" in the configuraton file\n";

  for (int i=0; i<numslm; i++) {
    int npar=4;
    char **par=new char*[npar];
    for (int j=0; j<4; j++) {
      par[j]=new char[BUFSIZ];
    }

    par[0][0]='\0';
    inp >> par[1] >> par[2] >> par[3];

    subtrainfile=NULL;
    slmtype=0;
    subprunefreq=-1;
    subprunesingletons=true;
    subprunetopsingletons=false;
    GetParams(&npar, &par, (char*) NULL);

    if (!slmtype || !subtrainfile || !subprunefreq==-1) {
      cerr << "slm incomplete parameters\n";
      exit(1);
    }

    switch (slmtype) {

    case LINEAR_WB:
      sublm[i]=new linearwb(subtrainfile,depth,subprunefreq,MSHIFTBETA_I);
      break;

    case SHIFT_BETA:
      sublm[i]=new shiftbeta(subtrainfile,depth,subprunefreq,-1,SHIFTBETA_I);
      break;

    case SHIFT_ONE:
      sublm[i]=new shiftbeta(subtrainfile,depth,subprunefreq,SIMPLE_I);
      break;

    case MOD_SHIFT_BETA:
      sublm[i]=new mshiftbeta(subtrainfile,depth,subprunefreq,MSHIFTBETA_I);
      break;

    case MIXTURE:
      sublm[i]=new mixture(usefulltable,subtrainfile,depth,subprunefreq);
      break;

    default:
      cerr << "not implemented yet\n";
      exit(1);
    };

    sublm[i]->prunesingletons(subprunesingletons==true);
    sublm[i]->prunetopsingletons(subprunetopsingletons==true);

    if (subprunetopsingletons==true)
      //apply most specific pruning method
      sublm[i]->prunesingletons(false);


    cerr << "eventually generate OOV code of sub lm[" << i << "]\n";
    sublm[i]->dict->genoovcode();

    //create super dictionary
    dict->augment(sublm[i]->dict);

    //creates the super n-gram table
    if(usefulltable) augment(sublm[i]);

  }
	
	cerr << "eventually generate OOV code of the mixture\n";
  dict->genoovcode();
  cerr << "dict size of the mixture:" << dict->size() << "\n";
  //tying parameters
  k1=2;
  k2=10;
};

double mixture::reldist(double *l1,double *l2,int n)
{
  double dist=0.0,size=0.0;
  for (int i=0; i<n; i++) {
    dist+=(l1[i]-l2[i])*(l1[i]-l2[i]);
    size+=l1[i]*l1[i];
  }
  return sqrt(dist/size);
}


double rand01()
{
  return (double)rand()/(double)RAND_MAX;
}

int mixture::genpmap()
{
  dictionary* d=sublm[0]->dict;

  cerr << "Computing parameters mapping: ..." << d->size() << " ";
  pm=new int[d->size()];
  //initialize
  for (int i=0; i<d->size(); i++) pm[i]=0;

  pmax=k2-k1+1; //update # of parameters

  for (int w=0; w<d->size(); w++) {
    int f=d->freq(w);
    if ((f>k1) && (f<=k2)) pm[w]=f-k1;
    else if (f>k2) {
      pm[w]=pmax++;
    }
  }
  cerr << "pmax " << pmax << " ";
  return 1;
}

int mixture::pmap(ngram ng,int lev)
{

  ngram h(sublm[0]->dict);
  h.trans(ng);

  if (lev<=1) return 0;
  //get the last word of history
  if (!sublm[0]->get(h,2,1)) return 0;
  return (int) pm[*h.wordp(2)];
}


int mixture::savepar(char* opf)
{
  mfstream out(opf,ios::out);

  cerr << "saving parameters in " << opf << "\n";
  out << lmsize() << " " << pmax << "\n";

  for (int i=0; i<=lmsize(); i++)
    for (int j=0; j<pmax; j++)
      out.writex(l[i][j],sizeof(double),numslm);


  return 1;
}


int mixture::loadpar(char* ipf)
{

  mfstream inp(ipf,ios::in);

  if (!inp) {
    cerr << "cannot open file with parameters: " << ipf << "\n";
    exit(1);
  }

  cerr << "loading parameters from " << ipf << "\n";

  // check compatibility
  char header[100];
  inp.getline(header,100);
  int value1,value2;
  sscanf(header,"%d %d",&value1,&value2);

  if (value1 != lmsize() || value2 != pmax) {
    cerr << "parameter file " << ipf << " is incompatible\n";
    exit(1);
  }

  for (int i=0; i<=lmsize(); i++)
    for (int j=0; j<pmax; j++)
      inp.readx(l[i][j],sizeof(double),numslm);

  return 1;
}

int mixture::train()
{

  double zf;

  srand(1333);

  genpmap();

  if (dub()<dict->size()) {
    cerr << "\nERROR: DUB value is too small: the LM will possibly compute wrong probabilities if sub-LMs have different vocabularies!\n";
		cerr << "This exception should already have been handled before!!!\n";
		exit(1);
  }

  cerr << "mixlm --> DUB: " << dub() << endl;
  for (int i=0; i<numslm; i++) {
    cerr << i << " sublm --> DUB: " << sublm[i]->dub()  << endl;
    cerr << "eventually generate OOV code ";
    cerr << sublm[i]->dict->encode(sublm[i]->dict->OOV()) << "\n";
    sublm[i]->train();
  }

  //initialize parameters

  for (int i=0; i<=lmsize(); i++) {
    l[i]=new double*[pmax];
    for (int j=0; j<pmax; j++) {
      l[i][j]=new double[numslm];
      for (int k=0; k<numslm; k++)
        l[i][j][k]=1.0/(double)numslm;
    }
  }

  if (ipfname) {
    //load parameters from file
    loadpar(ipfname);
  } else {
    //start training of mixture model

    double oldl[pmax][numslm];
    char alive[pmax],used[pmax];
    int totalive;

    ngram ng(sublm[0]->dict);

    for (int lev=1; lev<=lmsize(); lev++) {

      zf=sublm[0]->zerofreq(lev);

      cerr << "Starting training at lev:" << lev << "\n";

      for (int i=0; i<pmax; i++) {
        alive[i]=1;
        used[i]=0;
      }
      totalive=1;
      int iter=0;
      while (totalive && (iter < 20) ) {

        iter++;

        for (int i=0; i<pmax; i++)
          if (alive[i])
            for (int j=0; j<numslm; j++) {
              oldl[i][j]=l[lev][i][j];
              l[lev][i][j]=1.0/(double)numslm;
            }

        sublm[0]->scan(ng,INIT,lev);
        while(sublm[0]->scan(ng,CONT,lev)) {

          //do not include oov for unigrams
          if ((lev==1) && (*ng.wordp(1)==sublm[0]->dict->oovcode()))
            continue;

          int par=pmap(ng,lev);
          used[par]=1;

          //controllo se aggiornare il parametro
          if (alive[par]) {

            double backoff=(lev>1?prob(ng,lev-1):1); //backoff
            double denom=0.0;
            double* numer = new double[numslm];
						double fstar,lambda;

            //int cv=(int)floor(zf * (double)ng.freq + rand01());
            //int cv=1; //old version of leaving-one-out
            int cv=(int)floor(zf * (double)ng.freq)+1;
            //int cv=1; //old version of leaving-one-out
            //if (lev==3)q

            //if (iter>10)
            // cout << ng
            // << " backoff " << backoff
            // << " level " << lev
            // << "\n";

            for (int i=0; i<numslm; i++) {

              //use cv if i=0

              sublm[i]->discount(ng,lev,fstar,lambda,(i==0)*(cv));
              numer[i]=oldl[par][i]*(fstar + lambda * backoff);

              ngram ngslm(sublm[i]->dict);
              ngslm.trans(ng);
              if ((*ngslm.wordp(1)==sublm[i]->dict->oovcode()) &&
                  (dict->dub() > sublm[i]->dict->size()))
                numer[i]/=(double)(dict->dub() - sublm[i]->dict->size());

              denom+=numer[i];
            }

            for (int i=0; i<numslm; i++) {
              l[lev][par][i]+=(ng.freq * (numer[i]/denom));
              //if (iter>10)
              //cout << ng << " l: " << l[lev][par][i] << "\n";
            }
						delete []numer;
          }
        }

        //normalize all parameters
        totalive=0;
        for (int i=0; i<pmax; i++) {
          double tot=0;
          if (alive[i]) {
            for (int j=0; j<numslm; j++) tot+=(l[lev][i][j]);
            for (int j=0; j<numslm; j++) l[lev][i][j]/=tot;

            //decide if to continue to update
            if (!used[i] || (reldist(l[lev][i],oldl[i],numslm)<=0.05))
              alive[i]=0;
          }
          totalive+=alive[i];
        }

        cerr << "Lev " << lev << " iter " << iter << " tot alive " << totalive << "\n";

      }
    }
  }

  if (opfname) savepar(opfname);


  return 1;
}

int mixture::discount(ngram ng_,int size,double& fstar,double& lambda,int /* unused parameter: cv */)
{

  ngram ng(dict);
  ng.trans(ng_);
	
  double lambda2,fstar2;
  fstar=0.0;
  lambda=0.0;
  int p=pmap(ng,size);
  assert(p <= pmax);
  double lsum=0;


  for (int i=0; i<numslm; i++) {
    sublm[i]->discount(ng,size,fstar2,lambda2,0);

    ngram ngslm(sublm[i]->dict);
    ngslm.trans(ng);

    if (dict->dub() > sublm[i]->dict->size()){
      if (*ngslm.wordp(1) == sublm[i]->dict->oovcode()) {
        fstar2/=(double)(sublm[i]->dict->dub() - sublm[i]->dict->size()+1);
      }
		}


    fstar+=(l[size][p][i]*fstar2);
    lambda+=(l[size][p][i]*lambda2);
    lsum+=l[size][p][i];
  }

  if (dict->dub() > dict->size())
    if (*ng.wordp(1) == dict->oovcode()) {
      fstar*=(double)(dict->dub() - dict->size()+1);
    }
	
  assert((lsum>LOWER_DOUBLE_PRECISION_OF_1) && (lsum<=UPPER_DOUBLE_PRECISION_OF_1));
  return 1;
}


//creates the ngramtable on demand from the sublm tables
int mixture::get(ngram& ng,int n,int lev)
{

	if (usefulltable)
	{
		return ngramtable::get(ng,n,lev);
	}
		
  //free current tree
  resetngramtable();

  //get 1-word prefix from ng
  ngram ug(dict,1);
  *ug.wordp(1)=*ng.wordp(ng.size);

  //local ngram to upload entries
  ngram locng(dict,maxlevel());

  //allocate subtrees from sublm
  for (int i=0; i<numslm; i++) {

    ngram subug(sublm[i]->dict,1);
    subug.trans(ug);

    if (sublm[i]->get(subug,1,1)) {

      ngram subng(sublm[i]->dict,maxlevel());
      *subng.wordp(maxlevel())=*subug.wordp(1);
      sublm[i]->scan(subug.link,subug.info,1,subng,INIT,maxlevel());
      while(sublm[i]->scan(subug.link,subug.info,1,subng,CONT,maxlevel())) {
        locng.trans(subng);
        put(locng);
      }
    }
  }

  return ngramtable::get(ng,n,lev);

}








