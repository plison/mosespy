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

#include <sys/mman.h>
#include <stdio.h>
#include <cmath>
#include <limits>
#include <string>
#include <sstream>
#include <pthread.h>
#include "thpool.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "n_gram.h"
#include "util.h"
#include "dictionary.h"
#include "ngramtable.h"
#include "doc.h"
#include "cswam.h"

#define BUCKET 1000
#define SSEED 1.0

using namespace std;

#define MY_RAND (((float)random()/RAND_MAX)* 2.0 - 1.0)
	
cswam::cswam(char* sdfile,char *tdfile, char* w2vfile,bool normvect,bool scalevect,bool trainvar){
    
    //create dictionaries
    srcdict=new dictionary(NULL,100000); srcdict->generate(sdfile,true);
    trgdict=new dictionary(NULL,100000); trgdict->generate(tdfile,true);
    
    //make aware of delimiters
    
    //load word2vec dictionary
    W2V=NULL;D=0;
    loadword2vec(w2vfile);

    //check consistency of word2vec with target vocabulary
    
    //actual model structure
    S=NULL;
    M=NULL;
    A=NULL;
    
    
    normalize_vectors=normvect;
    scale_vectors=scalevect;
    train_variances=trainvar;
    
    srandom(100); //ensure repicable generation of random numbers
    bucket=BUCKET;
    threads=1;
}

cswam::~cswam() {

    assert(A==NULL);
    
    if (S){
        cerr << "Releasing memory of S\n";
        for (int e=0;e<trgdict->size();e++) delete [] S[e];
        delete S;
    }
    if (M){
        cerr << "Releasing memory of M\n";
        for (int e=0;e<trgdict->size();e++) delete [] M[e];
        delete [] M;
    }
    if (W2V){
        cerr << "Releasing memory of W2W\n";
        for (int e=0;e<srcdict->size();e++) delete [] W2V[e];
        delete []W2V;
    }
    
   delete srcdict; delete trgdict;
    
    
}



void cswam::loadword2vec(char* fname){
    
    cerr << "Loading word2vec file " << fname << " ...\n";
    mfstream inp(fname,ios::in);
    
    long long w2vsize;
    inp >> w2vsize; cout << w2vsize << "\n";
    inp >> D ; cout << D  << "\n";
    
    W2V=new float* [srcdict->size()];
    for (int f=0;f<srcdict->size();f++) W2V[f]=NULL;
    
    char word[100]; int code; float dummy;
    
    for (long long i=0;i<w2vsize;i++){
        inp >> word;
        code=srcdict->encode(word);
        if (code != srcdict->oovcode()){
            W2V[code]=new float[D];
            for (int d=0;d<D;d++) inp >> W2V[code][d];
        }
        else //skip this word
            for (int d=0;d<D;d++) inp >> dummy;
        
        if (!(i % 100000)) cerr<< word << " ";
    }
    
    for (int f=0;f<srcdict->size();f++){
        if (W2V[f]==NULL){
            cerr << "Missing src word in w2v: " << srcdict->decode(f) << "\n";
            W2V[f]=new float[D];
            for (int d=0;d<D;d++) W2V[f][d]=0;
        }
    }
    
    
    cerr << "\n";
    
    //normalized vector components
    float mean[D]; memset(mean,0,D*sizeof(float));
    float var[D]; memset(var,0,D*sizeof(float));
    
    for (code=0;code<srcdict->size();code++){
        
        if (!W2V[code]){
            cerr << "creating vector for word " << srcdict->decode(code) << "\n";
            W2V[code]=new float[D];
            for (int d=0;d<D;d++) W2V[code][d]=0; //to be worked out!
        }
        
        if (normalize_vectors || scale_vectors)
            for (int d=0;d<D;d++){
                mean[d]+=W2V[code][d];
                var[d]+=(W2V[code][d] * W2V[code][d]);
            }
    }
    
    if (normalize_vectors || scale_vectors){
        
        for (int d=0;d<D;d++){
            mean[d]/=srcdict->size();
            var[d]=var[d]/srcdict->size() - (mean[d]*mean[d]);
            cerr << d << " mean: " << mean[d] << "  sd: " << sqrt(var[d]) << "\n";
        }
        
        if (normalize_vectors){
            cerr << "Shifting vectors\n";
            for (code=0;code<srcdict->size();code++)
                for (int d=0;d<D;d++) W2V[code][d]=(W2V[code][d] - mean[d]);
        }
        if (normalize_vectors || scale_vectors){
            cerr << "Scaling vectors\n";
            for (code=0;code<srcdict->size();code++)
                for (int d=0;d<D;d++) W2V[code][d]=W2V[code][d]/sqrt(var[d]);
            
        }
    }
    cerr << " ... done\n";
};


void cswam::initModel(char* modelfile){
    
    //test if model is readable
    bool testmodel=false;
    FILE* f;if ((f=fopen(modelfile,"r"))!=NULL){fclose(f);testmodel=true;}
    
    if (testmodel) loadModel(modelfile,true); //we are in training mode!
    else{ //initialize model
        M=new float* [trgdict->size()];
        S=new float* [trgdict->size()];
        for (int e=0; e<trgdict->size(); e++){
            M[e]=new float [D];
            S[e]=new float [D];
            //initialize with w2v value if the same word is also in src
            int f=srcdict->encode(trgdict->decode(e));
            if (f!=srcdict->oovcode()){
                memcpy(M[e],W2V[f],sizeof(float) * D);
                for (int d=0;d<D;d++) S[e][d]=SSEED/4;
                cout << "initialize: " << srcdict->decode(f) << "\n";
            }else
                for (int d=0;d<D;d++){
                    M[e][d]=0.0; //pick mean zero
                    S[e][d]=SSEED; //take a wide standard deviation
                }
        }
    }
}

int cswam::saveModelTxt(char* fname){
    cerr << "Writing model into: " << fname << "\n";
    mfstream out(fname,ios::out);
    for (int e=0; e<trgdict->size(); e++){
        out << trgdict->decode(e) <<"\n";
        for (int d=0;d<D;d++) out << M[e][d] << " ";out << "\n";
        for (int d=0;d<D;d++) out << S[e][d] << " ";out << "\n";
    }
    return 1;
}

int cswam::saveModel(char* fname){
    cerr << "Saving model into: " << fname << " ...";
    mfstream out(fname,ios::out);
    out << "CSWAM " << D << "\n";
    trgdict->save(out);
    for (int e=0; e<trgdict->size(); e++){
        out.write((const char*)M[e],sizeof(float) * D);
        out.write((const char*)S[e],sizeof(float) * D);
    }
    out.close();
    cerr << "\n";
    return 1;
}

int cswam::loadModel(char* fname,bool expand){

    cerr << "Loading model from: " << fname << "...";
    mfstream inp(fname,ios::in);
    char header[100];
    inp.getline(header,100);
    cerr << header ;
    int r;
    sscanf(header,"CSWAM %d\n",&r);
    if (D>0 && r != D)
        exit_error(IRSTLM_ERROR_DATA, "incompatible dimension in model");
    else
        D=r;

    cerr << "\nLoading dictionary ... ";
    dictionary* dict=new dictionary(NULL,1000000);
    dict->load(inp);
    dict->encode(dict->OOV());
    int current_size=dict->size();
    
    //expand the model for training or keep the model fixed for testing
    if (expand){
        cerr << "\nExpanding model to include targer dictionary";
        dict->incflag(1);
        for (int code=0;code<trgdict->size();code++)
            dict->encode(trgdict->decode(code));
        dict->incflag(2);
    }
    //replace the trgdict with the model dictionary
    delete trgdict;trgdict=dict;
    
    M=new float* [trgdict->size()];
    S=new float* [trgdict->size()];
    for (int e=0; e<trgdict->size(); e++){
        M[e]=new float [D];
        S[e]=new float [D];
    }
    
    cerr << "\nReading parameters .... ";
    for (int e=0; e<current_size; e++){
        inp.read((char *)M[e],sizeof(float) * D);
        inp.read((char *)S[e],sizeof(float) * D);
    }
    inp.close();
    cerr << "\nInitializing " << trgdict->size()-current_size << " new entries .... ";
    for (int e=current_size; e<trgdict->size(); e++)
        for (int d=0;d<D;d++){
            M[e][d]=0.0;S[e][d]=SSEED;
        }
    
    cerr << "\nDone\n";
    return 1;
}

void cswam::initAlpha(){

    //install Alpha[s][i][j] to collect counts
    //allocate if empty
    
    if (A==NULL){
        assert(trgdata->numdoc()==srcdata->numdoc());
        A=new float**[trgdata->numdoc()];
        for (int s=0;s<trgdata->numdoc();s++){
            A[s]=new float *[trgdata->doclen(s)];
             for (int i=0;i<trgdata->doclen(s);i++)
                A[s][i]=new float [srcdata->doclen(s)];
        }
    }
    //initialize
    for (int s=0;s<trgdata->numdoc();s++){
        for (int i=0;i<trgdata->doclen(s);i++)
            memset(A[s][i],0,sizeof(float) * srcdata->doclen(s));
    }
}

void cswam::freeAlpha(){

    if (A!=NULL){
        for (int s=0;s<trgdata->numdoc();s++){
            for (int i=0;i<trgdata->doclen(s);i++)
                delete [] A[s][i];
            delete [] A[s];
        }
        delete [] A;
        A=NULL;
    }
}

///*****
pthread_mutex_t mut1;
pthread_mutex_t mut2;
double LL=0; //Log likelihood


float logsum(float a,float b){
    if (b<a) return a + log(1 + exp(b-a));
    else return b + log(1+ exp(a-b));
}


float cswam::LogGauss(const int dim,const float* x,const float *m, const float *s){
    
    static float log2pi=1.83787; //log(2 pi)
    float dist=0; float norm=0;

    for (int i=0;i<dim;i++){
        dist=(x[i]-m[i])*(x[i]-m[i])/(s[i]);
        norm+=s[i];
    }
    
    return -0.5 * (dist + dim * log2pi + logf(norm));
    
}
            
void cswam::expected_counts(void *argv){
    
    long long s=(long long) argv;
    
    if (! (s % 10000)) {cerr << ".";cerr.flush();}
    //fprintf(stderr,"Thread: %lu  sentence: %d  (out of %d)\n",(long)pthread_self(),s,srcdata->numdoc());
    
    int trglen=trgdata->doclen(s); // length of target sentence
    int srclen=srcdata->doclen(s); //length of source sentence
    
    float den;
   
    //compute denominator for each source-target pair
    for (int j=0;j<srclen;j++){
        //cout << "j: " << srcdict->decode(srcdata->docword(s,j)) << "\n";

          for (int i=0;i<trglen;i++){
            A[s][i][j]=LogGauss(D, W2V[srcdata->docword(s,j)],
                                M[trgdata->docword(s,i)],
                                S[trgdata->docword(s,i)]);
            
              if (i==0) den=A[s][i][j];
              else den=logsum(den,A[s][i][j]);
          }
        
        for (int i=0;i<trglen;i++){
            assert(A[s][i][j]<= den);
            A[s][i][j]=expf(A[s][i][j]-den); // A is now a regular expected count
            
            if (A[s][i][j]<0.000000001) A[s][i][j]=0; //take mall risk of wrong normalization
            
            //            if (trgdata->docword(s,i)==trgdict->encode("documentos"))
            
//            cout << "Pr(" << trgdict->decode(trgdata->docword(s,i))
//            << " | "  << srcdict->decode(srcdata->docword(s,j))
//            << ") = " << A[s][i][j] << "\n";
        }
    }
}

void cswam::maximization(void *argv){
    
    long long d=(long long) argv;
    
    if (!(d  % 10)) cerr <<".";
    //Maximization step: Mean;
    for (int s=0;s<srcdata->numdoc();s++)
        for (int i=0;i<trgdata->doclen(s);i++)
            for (int j=0;j<srcdata->doclen(s);j++)
                if (A[s][i][j]>0)
                    M[trgdata->docword(s,i)][d]+=A[s][i][j] * W2V[srcdata->docword(s,j)][d];
    
    //second pass
    for (int e=0;e<trgdict->size();e++) if (Den[e]>0) M[e][d]/=Den[e];
    
    if (train_variances){
        //Maximization step: Variance;
        
        for (int s=0;s<srcdata->numdoc();s++)
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int j=0;j<srcdata->doclen(s);j++)
                    if (A[s][i][j]>0)
                        S[trgdata->docword(s,i)][d]+=
                        (A[s][i][j] *
                         (W2V[srcdata->docword(s,j)][d]-M[trgdata->docword(s,i)][d]) *
                         (W2V[srcdata->docword(s,j)][d]-M[trgdata->docword(s,i)][d])
                         );
        
        //second pass
        for (int e=0;e<trgdict->size();e++){
            if (Den[e]>0){
                S[e][d]/=Den[e];
                if (S[e][d] < 0.01) S[e][d]=0.01;
            }
            else
                if (d==0) cout << "-\b";
        }
    }
}


int cswam::train(char *srctrainfile, char*trgtrainfile,char *modelfile, int maxiter,int threads){
    
   
    initModel(modelfile);

    //Load training data

    srcdata=new doc(srcdict,srctrainfile);
    trgdata=new doc(trgdict,trgtrainfile);

   
    int iter=0;
    
    cerr << "Starting training";
    threadpool thpool=thpool_init(threads);
    task *t=new task[srcdata->numdoc()];
    assert(srcdata->numdoc()>D); //multi-threading also distributed over D
    
   
    //support variable to compute model denominator
    Den=new float[trgdict->size()];
    
    while (iter < maxiter){
        
        cerr << "\nIteration: " << ++iter <<  "\n";
        
        initAlpha();
        
        cerr << "E-step: ";
        //compute expected counts in each single sentence
        for (long long  s=0;s<srcdata->numdoc();s++){
            //prepare and assign tasks to threads
            t[s].ctx=this; t[s].argv=(void *)s;
            thpool_add_work(thpool, &cswam::expected_counts_helper, (void *)&t[s]);
            
        }
        //join all threads
        thpool_wait(thpool);
        
        
        //Prepare for model for update
        for (int e=0;e <trgdict->size();e++){
            memset(M[e],0,D * sizeof (float));
            if (train_variances)
                memset(S[e],0,D * sizeof (float)); //keep variance constant
        }
        
        memset(Den,0,trgdict->size() * sizeof(float));
        
        
        //compute normalization term for each target word
        for (int s=0;s<srcdata->numdoc();s++)
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int j=0;j<srcdata->doclen(s);j++)
                    Den[trgdata->docword(s,i)]+=A[s][i][j];
        
        cerr << "\nM-step: ";
        for (long long d=0;d<D;d++){
            t[d].ctx=this; t[d].argv=(void *)d;
            thpool_add_work(thpool, &cswam::maximization_helper, (void *)&t[d]);
        }
        
        //join all threads
        thpool_wait(thpool);
        
        
        
        if (srcdata->numdoc()> 10) system("date");
        
        saveModel(modelfile);
        saveModelTxt("modelfile.txt");
    }
    
   // for (int e=0;e<trgdict->size();e++)
   //     for (int d=0;d<D;d++)
   //         cout << trgdict->decode(e) << " S: " << S[e][d] << " M: " << M[e][d]<< "\n";

    //destroy thread pool
    thpool_destroy(thpool);
  
    freeAlpha();

    delete []Den;
    delete srcdata; delete trgdata;
    delete [] t;
    
    return 1;
}



void cswam::aligner(void *argv){
    long long s=(long long) argv;
    static float maxfloat=std::numeric_limits<float>::max();

    
    if (! (s % 10000)) {cerr << ".";cerr.flush();}
    //fprintf(stderr,"Thread: %lu  Document: %d  (out of %d)\n",(long)pthread_self(),s,srcdata->numdoc());
    
    int trglen=trgdata->doclen(s); // length of target sentence
    int srclen=srcdata->doclen(s); //length of source sentence
    
    assert(trglen<MAX_LINE);
    
    //Viterbi alignment: find the most probable alignment for source
    float score; float best_score;int best_j;
    for (int i=0;i<trglen;i++){
        best_score=-maxfloat;best_j=0;
        //cout << trgdict->decode(trgdata->docword(s,i)) << "\n";
        for (int j=0;j<srclen;j++){
            score=LogGauss(D,
                     W2V[srcdata->docword(s,j)],
                     M[trgdata->docword(s,i)],
                     S[trgdata->docword(s,i)]);
          //  cout << "\t " << srcdict->decode(srcdata->docword(s,j)) << "  " << dist << "\n";
            //if (dist > -50) score=(float)exp(-dist)/norm;
            if (score > best_score){
                best_score=score;
                best_j=j;
            }
        }
        alignments[s % bucket][i]=best_j;
    }
}




int cswam::test(char *srctestfile, char *trgtestfile, char* modelfile, char* alignfile,int threads){
    
    {mfstream out(alignfile,ios::out);} //empty the file
    
    initModel(modelfile);
    
    //Load training data
    srcdata=new doc(srcdict,srctestfile);
    trgdata=new doc(trgdict,trgtestfile);
    assert(srcdata->numdoc()==trgdata->numdoc());
    
   
    bucket=BUCKET; //initialize the bucket size
    
    alignments=new int* [BUCKET];
    for (int s=0;s<BUCKET;s++)
        alignments[s]=new int[MAX_LINE];
    
    threadpool thpool=thpool_init(threads);
    task *t=new task[bucket];
    
    cerr << "Start alignment\n";
    
    for (long long s=0;s<srcdata->numdoc();s++){
        
        t[s % bucket].ctx=this; t[s % bucket].argv=(void *)s;
        thpool_add_work(thpool, &cswam::aligner_helper, (void *)&t[s % bucket]);
        

        if (((s % bucket) == (bucket-1)) || (s==(srcdata->numdoc()-1)) ){
            //join all threads
            thpool_wait(thpool);
            
            //cerr << "Start printing\n";
            
            if ((s % bucket) != (bucket-1))
                    bucket=srcdata->numdoc() % bucket; //last bucket at end of file
            
                mfstream out(alignfile,ios::out | ios::app);
                
                for (int b=0;b<bucket;b++){ //includes the eof case of
                    out << "Sentence: " << s-bucket+1+b;
                    for (int i=0; i<trgdata->doclen(s-bucket+1+b); i++) out << " "  << i << "-" << alignments[b][i];
                    out << "\n";
                }
        }
        
    }
    
    
    //destroy thread pool
    thpool_destroy(thpool);
    
    delete [] t;
    for (int s=0;s<BUCKET;s++) delete [] alignments[s];delete [] alignments;
    delete srcdata; delete trgdata;
    return 1;
}

