/******************************************************************************
 IrstLM: IRST Language Model Toolkit, compile LM
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

 **********************************************dou********************************/

using namespace std;

#include <cmath>
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "n_gram.h"
#include "util.h"
#include "dictionary.h"
#include "ngramtable.h"
#include "doc.h"
#include "cplsa.h"

#define MY_RAND (((double)rand()/RAND_MAX)* 2.0 - 1.0)

plsa::plsa(dictionary* dictfile,int top,
           char* baseFile,char* featFile,char* hFile,char* wFile,char* tFile)
{

  dict = dictfile;

  topics=top;

  assert (topics>0);

  W=new double* [dict->size()+1];
  for (int i=0; i<(dict->size()+1); i++) W[i]=new double [topics];

  T=new double* [dict->size()+1];
  for (int i=0; i<(dict->size()+1); i++) T[i]=new double [topics];

  H=new double [topics];

  basefname=baseFile;
  featfname=featFile;

  tfname=tFile;
  wfname=wFile;
  hinfname=new char[BUFSIZ];
  sprintf(hinfname,"%s",hFile);

  houtfname=new char[BUFSIZ];
  sprintf(houtfname,"%s.out",hinfname);
  cerr << "Hfile in:" << hinfname << " out:" << houtfname << "\n";
}

int plsa::initW(double noise,int spectopic)
{

  FILE *f;

  if (wfname && ((f=fopen(wfname,"r"))!=NULL)) {
    fclose(f);
    loadW(wfname);
  } else {

    if (spectopic) {
      //special topic 0: first st words from dict
      double TotW=0;
      for (int i=0; i<spectopic; i++)
        TotW+=W[i][0]=dict->freq(i);
      for (int i=0; i<(dict->size()+1); i++)
        W[i][0]/=TotW;
    }

    for (int t=(spectopic?1:0); t<topics; t++) {
      double TotW=0;
      for (int i=0; i<(dict->size()+1); i++)
        TotW+=W[i][t]=1 + noise * MY_RAND;
      for (int i=0; i<(dict->size()+1); i++)
        W[i][t]/=TotW;
    }
  }
  return 1;
}

int plsa::initH(double noise,int n)
{

  FILE *f;

  if ((f=fopen(hinfname,"r"))==NULL) {
    mfstream hinfd(hinfname,ios::out);
    for (int j=0; j<n; j++) {
      double TotH=0;
      for (int t=0; t<topics; t++) TotH+=H[t]=1+noise * MY_RAND;
      for (int t=0; t<topics; t++) H[t]/=TotH;
      hinfd.write((const char*)H,topics *sizeof(double));
    }
    hinfd.close();
  } else
    fclose(f);
  return 1;
}

int plsa::saveWtxt(char* fname)
{

  mfstream out(fname,ios::out);
  out << topics << "\n";
  for (int i=0; i<dict->size(); i++) {
    out << dict->decode(i) << " " << dict->freq(i);
    double totW=0;
    for (int t=0; t<topics; t++) totW+=W[i][t];
    out <<"totPr:" << totW << ":";
    for (int t=0; t<topics; t++)
      out << " " << W[i][t];
    out << "\n";
  }
  out.close();
  return 1;
}

int plsa::saveW(char* fname)
{

  mfstream out(fname,ios::out);
  out.write((const char*)&topics,sizeof(int));
  for (int i=0; i<dict->size(); i++)
    out.write((const char*)W[i],sizeof(double)*topics);
  out.close();
  return 1;
}

int plsa::saveT(char* fname)
{
  mfstream out(fname,ios::out);
  out.write((const char*)&topics,sizeof(int));
  for (int i=0; i<dict->size(); i++) {
    double totT=0.0;
    for (int t=0; t<topics; t++) totT+=T[i][t];
    if (totT>0.00001) {
      out.write((const char*)&i,sizeof(int));
      out.write((const char*)T[i],sizeof(double)*topics);
    }
  }
  out.close();
  return 1;
}


int plsa::combineT(char* tlist)
{

  double *tvec=new double[topics];
  int w;
  int to;
  char fname[1000];
  for (int i=0; i<dict->size(); i++)
    for (int t=0; t<topics; t++) T[i][t]=0;

  mfstream inp(tlist,ios::in);
  while (inp >> fname) {
    mfstream tin(fname,ios::in);
    tin.read((char *)&to,sizeof(int));
    assert(to==topics);
    while(!tin.eof()) {
      tin.read((char *)&w,sizeof(int));
      tin.read((char *)tvec,sizeof(double)*topics);
      for (int t=0; t<topics; t++) T[w][t]+=tvec[t];
    }
    tin.close();
  }

  delete [] tvec;

  for (int t=0; t<topics; t++) {
    double Tsum=0;
    for (int i=0; i<dict->size(); i++) {
      if (T[i][t]==0.0) T[i][t]=1e-10; //add some noise
      Tsum+=T[i][t];
    }
    for (int i=0; i<dict->size(); i++) W[i][t]=T[i][t]/Tsum;
  }
  //check
  return 1;
}

int plsa::loadW(char* fname)
{
  int r;
  mfstream inp(fname,ios::in);
  inp.read((char *)&r,sizeof(int)); //number of topics

  if (topics>0 && r != topics) {
    cerr << "incompatible number of topics: " << r << "\n";
    exit(2);
  } else
    topics=r;

  for (int i=0; i<dict->size(); i++)
    inp.read((char *)W[i],sizeof(double)*topics);

  return 1;
}

int plsa::saveFeat(char* fname)
{

  //compute distribution on doc 0
  double *WH=new double [dict->size()];
  for (int i=0; i<dict->size(); i++) {
    WH[i]=0;
    for (int t=0; t<topics; t++)
      WH[i]+=W[i][t]*H[t];
  }

  double maxp=WH[0];
  for (int i=1; i<dict->size(); i++)
    if (WH[i]>maxp) maxp=WH[i];

  cerr << "Get max prob" << maxp << "\n";

  mfstream out(fname,ios::out);
  ngramtable ngt(NULL,1,NULL,NULL,NULL,0,0,NULL,0,COUNT);
  ngt.dict->incflag(1);

  ngram ng(dict,1);
  ngram ng2(ngt.dict,1);

  for (int i=0; i<dict->size(); i++) {
    *ng.wordp(1)=i;
    ng.freq=(int)floor((WH[i]/maxp) * 1000000);
    if (ng.freq) {
      ng2.trans(ng);
      ng2.freq=ng.freq;
      //cout << ng << "\n" << ng2 << "\n";
      ngt.put(ng2);
      ngt.dict->incfreq(*ng2.wordp(1),ng2.freq);
    }
  }

  ngt.dict->incflag(0);
  ngt.savetxt(fname,1,1);// save in google format

  return 1;
}


int plsa::train(char *trainfile,int maxiter,double noiseH,int flagW,double noiseW,int spectopic)
{

  int dsize=dict->size(); //includes possible OOV

  srand(100);

  if (flagW) {
    //intialize W
    initW(noiseW,spectopic);
  }

  doc trset(dict,trainfile);
  trset.open(); //n is known

  initH(noiseH,trset.n);

  //support array
  double *WH=new double [dsize];

  //command
  char cmd[100];
  sprintf(cmd,"mv %s %s",houtfname,hinfname);

  //start of training

  double lastLL=10;
  double LL=-1e+99;

  int iter=0;
  int r=topics;


  while (iter < maxiter)
    //while ( (iter < maxiter) && (((lastLL-LL)/lastLL)>0.00001))
  {
    lastLL=LL;
    LL=0;

    if (flagW)  //reset support arrays
      for (int i=0; i<dict->size(); i++)
        for (int t=0; t<r; t++)
          T[i][t]=0;

    {

      mfstream hindf(hinfname,ios::in);
      mfstream houtdf(houtfname,ios::out);

      while(trset.read()) { //read next doc

        int m=trset.m;
        /* unused parameter       int n=trset.n;   */
        int j=trset.cd; //current document
        int N=0; // doc length

        //resume H
        hindf.read((char *)H,topics * sizeof(double));

        //precompute WHij i=0,...,m-1; j=n-1 fixed
        for (int i=0; i<m; i++) {
          WH[trset.V[i]]=0;
          N+=trset.N[trset.V[i]];
          for (int t=0; t<r; t++)
            WH[trset.V[i]]+=W[trset.V[i]][t]*H[t];
          LL+=trset.N[trset.V[i]] * log( WH[trset.V[i]] );
        }

        //UPDATE Tia
        if (flagW) {
          for (int i=0; i<m; i++) {
            for (int t=0; t<r; t++)
              T[trset.V[i]][t]+=
                (trset.N[trset.V[i]] * W[trset.V[i]][t] *
                 H[t]/WH[trset.V[i]]);
          }
        }

        //UPDATE Haj
        double totH=0;
        for (int t=0; t<r; t++) {
          double tmpHaj=0;
          for (int i=0; i<m; i++)
            tmpHaj+=(trset.N[trset.V[i]] * W[trset.V[i]][t] *
                     H[t]/WH[trset.V[i]]);
          H[t]=tmpHaj/(double)N;
          totH+=H[t];
        }

        if(totH>UPPER_SINGLE_PRECISION_OF_1 || totH<LOWER_SINGLE_PRECISION_OF_1) {
          cerr << "totH=" << totH << "\n";
          exit(1);
        }

        //save H
        houtdf.write((const char*)H,topics * sizeof(double));

        // start a new document
        if (!(j % 10000)) cerr << ".";

      }

      hindf.close();
      houtdf.close();

      cerr << cmd <<"\n";
      system(cmd);
    }


    if (flagW) {
      cerr <<"end of train file final update of Wia\n";
      for (int t=0; t<r; t++) {
        double Tsum=0;
        for (int i=0; i<dsize; i++) Tsum+=T[i][t];
        for (int i=0; i<dsize; i++) W[i][t]=T[i][t]/Tsum;
        cerr << "end of normalization\n";
      }
    }
    trset.reset();

    cout << "iteration: " << ++iter << " LL: " << LL << "\n";
    //	LL=lastLL;

    if (flagW) {
      cerr << "Saving base distributions\n";
      if (tfname) saveT(tfname);
      else saveW(basefname);
    }

  }

  if (!flagW) {
    cout << "Saving features\n";
    saveFeat(featfname);
  }

  delete [] WH;
  return 1;
}







