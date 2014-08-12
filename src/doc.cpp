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

******************************************************************************/


#include <math.h>
#include <assert.h>
#include "util.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "dictionary.h"
#include "n_gram.h"
#include "doc.h"

using namespace std;

doc::doc(dictionary* d,char* docfname)
{
  dict=d;
  n=0;
  m=0;
  V=new int[dict->size()];
  N=new int[dict->size()];
  T=new int[dict->size()];
  cd=-1;
  dfname=docfname;
  df=NULL;
  binary=false;
};

doc::~doc()
{
  delete [] V;
  delete [] N;
  delete [] T;
}



int doc::open()
{

  df=new mfstream(dfname,ios::in);

  char header[100];
  df->getline(header,100);
  if (sscanf(header,"DoC %d",&n) && n>0)
    binary=true;
  else if (sscanf(header,"%d",&n) && n>0)
    binary=false;
  else {
    exit_error(IRSTLM_ERROR_DATA, "doc::open() error: wrong header\n");
  }

  cerr << "opening: " << n << (binary?" bin-":" txt-") << "docs\n";
  cd=-1;

  return 1;
}


int doc::reset()
{

  cd=-1;
  m=0;
  df->close();
  delete df;
  open();
  return 1;
}


int doc::read()
{
    static int eod=dict->encode(dict->EoD());
    static int bod=dict->encode(dict->BoD());
    static int w=0;

  if (cd >=(n-1))
    return 0;

  m=0;
  
  memset(N,0,dict->size()*sizeof(int));
  //for (int i=0; i<dict->size(); i++) N[i]=0;

  if (binary) {
    df->read((char *)&m,sizeof(int));
    df->read((char *)V,m * sizeof(int));
    df->read((char *)T,m * sizeof(int));
    for (int i=0; i<m; i++) N[V[i]]=T[i];
  } else {

    ngram ng(dict);

    while((*df) >> ng) {
      w=*ng.wordp(1);
      if (ng.size>0) {
        if (w==bod) {
          ng.size=0;
          continue;
        }
        if (w==eod) {
          ng.size=0;
          break;
        }
        N[w]++;
        if (N[w]==1) V[m++]=w; //new word
      }
    }
  }
  cd++;
  return 1;
}


int doc::savernd(char* fname,int num)
{

  assert((df!=NULL) && (cd==-1));

  srand(100);

  mfstream out(fname,ios::out);
  out << "DoC\n";
  out.write((const char*) &n,sizeof(int));

  cerr << "n=" << n << "\n";

  //first select num random docs
  char taken[n];
  int r;
  for (int i=0; i<n; i++) taken[i]=0;

  for (int d=0; d<num; d++) {
    while((r=(rand() % n)) && taken[r]) {};
    cerr << "random document found " << r << "\n";
    taken[r]++;
    reset();
    for (int i=0; i<=r; i++) read();
    out.write((const char *)&m,sizeof(int));
    out.write((const char*) V,m * sizeof(int));
    for (int i=0; i<m; i++)
      out.write((const char*) &N[V[i]],sizeof(int));
  }

  //write the rest of files
  reset();
  for (int d=0; d<n; d++) {
    read();
    if (!taken[d]) {
      out.write((const char*)&m,sizeof(int));
      out.write((const char*)V,m * sizeof(int));
      for (int i=0; i<m; i++)
        out.write((const char*)&N[V[i]],sizeof(int));
    } else {
      cerr << "do not save doc " << d << "\n";
    }
  }
  //out.close();

  reset();
  return 1;
}

int doc::save(char* fname)
{

  assert((df!=NULL) && (cd==-1));

  mfstream out(fname,ios::out);
  out << "DoC "<< n << "\n";
  for (int d=0; d<n; d++) {
    read();
    out.write((const char*)&m,sizeof(int));
    out.write((const char*)V,m * sizeof(int));
    for (int i=0; i<m; i++)
      out.write((const char*)&N[V[i]],sizeof(int));
  }
  //out.close();

  reset();
  return 1;
}

int doc::save(char* fname, int nbins)
{

  assert((df!=NULL) && (cd==-1));
  //compute size of bin
  int bsz=(int)ceil((double)n / (double)nbins);
  char name[100];
  int i=0;

  while (cd < (n-1)) { // at least one document
    sprintf(name,"%s.%d",fname,++i);
    mfstream out(name,ios::out);
    int csz=(cd+bsz)<n?bsz:(n-cd-1);
    out << "DoC "<< csz << "\n";
    for (int d=0; d<csz; d++) {
      read();
      out.write((const char*)&m,sizeof(int));
      out.write((const char*)V,m * sizeof(int));
      for (int i=0; i<m; i++)
         out.write((const char*)&N[V[i]],sizeof(int));
    }
    out.close();
  }

  reset();
  return 1;
}








