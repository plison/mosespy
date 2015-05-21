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
#define SSEED 5.0

using namespace std;

#define MY_RAND (((float)random()/RAND_MAX)* 2.0 - 1.0)
	
cswam::cswam(char* sdfile,char *tdfile, char* w2vfile){
    
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
        
        if (!(i % 10000)) cout << word << "\n";
    }
    
    for (code=0;code<srcdict->size();code++)
        if (!W2V[code]){
            cerr << "creating vector for word " << srcdict->decode(code) << "\n";
            W2V[code]=new float[D];
            for (int d=0;d<D;d++) W2V[code][d]=0;
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
const float threshold1=0.00001;
const float threshold2=0.0001;


//compute gaussian with diagonal covariance matrix

void GaussArg(const int dim,const float* x,const float *m, const float *s,double &dist,double &norm){
    double twopi=6.28;
    dist=0;norm=1;
    for (int i=0;i<dim;i++){
        dist+=(x[i]-m[i])*(x[i]-m[i])/(s[i]);
        norm*=s[i];
    }
    //assert(dist>=0 && norm>0);
    dist*=0.5;
    norm=sqrt(twopi * norm);
}

            
void cswam::expected_counts(void *argv){
    
    long long s=(long long) argv;
    
    if (! (s % 10000)) {cerr << ".";cerr.flush();}
    //fprintf(stderr,"Thread: %lu  sentence: %d  (out of %d)\n",(long)pthread_self(),s,srcdata->numdoc());
    
    
    int trglen=trgdata->doclen(s); // length of target sentence
    int srclen=srcdata->doclen(s); //length of source sentence
    
    float den[srclen];
    float p[srclen][trglen];
    double dist[trglen]; double norm[trglen];double maxnorm; double mindist=D*10;
    //compute denominator for each source-target pair
    for (int j=0;j<srclen;j++){
        //cout << "j: " << srcdict->decode(srcdata->docword(s,j)) << "\n";
        maxnorm=0;mindist=D*10;
        for (int i=0;i<trglen;i++){
            GaussArg(D,
                     W2V[srcdata->docword(s,j)],
                     M[trgdata->docword(s,i)],
                     S[trgdata->docword(s,i)],dist[i],norm[i]);
            if (norm[i]>maxnorm) maxnorm=norm[i];
            if (dist[i]<mindist) mindist=dist[i];
        }
        //compute scaled likelihood and scaled denominator
        //cout << "maxnorm: " << maxnorm << "\n";
        den[j]=0;
        for (int i=0;i<trglen;i++){
            //cout << "i: " << trgdict->decode(trgdata->docword(s,i)) << " dist: " << -dist[i] << " norm: " << maxnorm/norm[i] << "\n";
            p[j][i]=(float)(exp(-dist[i]+mindist) * maxnorm/norm[i]);
            if (p[j][i] < 0.00001) p[j][i]=0;
            den[j]+=p[j][i];
        }
    }
    //
    
    for (int j=0;j<srclen;j++)
        for (int i=0;i<trglen;i++)
                    A[s][i][j]=p[j][i]/den[j];
    
    
                //cout << "Pr(" << trgdict->decode(trgdata->docword(s,i))
                //    << " | "  << srcdict->decode(srcdata->docword(s,j))
                //    << ") = " << A[s][i][j] << "\n";
    
    
}



int cswam::train(char *srctrainfile, char*trgtrainfile,char *modelfile, int maxiter,int threads){
    
    //check if to either use the dict of the modelfile
    //or create a new one from the data
    //load training data!
    
    
    //Initialize W matrix and load training data
    //notice: if dict is empy, then upload from model
    initModel(modelfile);

    //Load training data
    srcdata=new doc(srcdict,srctrainfile);
    trgdata=new doc(trgdict,trgtrainfile);
    
    int iter=0;
    
    cerr << "Starting training \n";
    threadpool thpool=thpool_init(threads);
    task *t=new task[srcdata->numdoc()];
    
    //pthread_mutex_init(&mut1, NULL);
    //pthread_mutex_init(&mut2, NULL);

    //support variable to compute model denominator
    float Den[trgdict->size()];
    
    while (iter < maxiter){
        LL=0;
        
        cerr << "Iteration: " << ++iter << " LL: " << LL << "\n";
        
        //initialize Alpha table (allocate first time)
        initAlpha();
        
        //for (int e=0;e<trgdict->size();e++)
        //    for (int d=0;d<D;d++)
        //        cout << trgdict->decode(e) << " S: " << S[e][d] << " M: " << M[e][d]<< "\n";
        
        
        //compute expected counts in each single sentence
        for (long long  s=0;s<srcdata->numdoc();s++){
            //prepare and assign tasks to threads
            t[s].ctx=this; t[s].argv=(void *)s;
            thpool_add_work(thpool, &cswam::expected_counts_helper, (void *)&t[s]);
            
        }
        //join all threads
        thpool_wait(thpool);
        
        //Maximization STEP
        
        
        //clear model
        for (int e=0;e <trgdict->size();e++){
            memset(M[e],0,D * sizeof (float));
            memset(S[e],0,D * sizeof (float));
        }
        
        memset(Den,0,trgdict->size() * sizeof(float));
        
        cerr << "Maximization step: Mean ";
        for (int s=0;s<srcdata->numdoc();s++){
            if (! (s % 10000)) cerr <<".";
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int j=0;j<srcdata->doclen(s);j++){
                    Den[trgdata->docword(s,i)]+=A[s][i][j];
                    if (A[s][i][j]>0)
                        for (int d=0;d<D;d++)
                            M[trgdata->docword(s,i)][d]+=A[s][i][j] * W2V[srcdata->docword(s,j)][d];
                    
                }
        }
        
        //second pass
        for (int e=0;e<trgdict->size();e++)
            if (Den[e]>0)
                for (int d=0;d<D;d++) M[e][d]/=Den[e];
        
        cerr << "\nMaximization step: Variance ";
        for (int s=0;s<srcdata->numdoc();s++){
            if (! (s % 10000)) cerr << ".";
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int j=0;j<srcdata->doclen(s);j++)
                    if (A[s][i][j]>0)
                        for (int d=0;d<D;d++)
                            S[trgdata->docword(s,i)][d]+=
                            (A[s][i][j] *
                             (W2V[srcdata->docword(s,j)][d]-M[trgdata->docword(s,i)][d]) *
                             (W2V[srcdata->docword(s,j)][d]-M[trgdata->docword(s,i)][d]));
        }
        
        //second pass
        for (int e=0;e<trgdict->size();e++)
            if (Den[e]>0)
                for (int d=0;d<D;d++){
                    S[e][d]/=Den[e];
                    //S[e][d]=SSEED;
                    if (S[e][d] < 0.01) S[e][d]=0.01;
                }
            else
                cout << "Skip " << trgdict->decode(e) << "\n";
        
        
        
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
    delete srcdata; delete trgdata;
    delete [] t;
    
    return 1;
}
//
//
//
//void cswam::aligner(void *argv){
//    long long d;
//    d=(long long) argv;
//    
//    if (! (d % 10000)) {cerr << ".";cerr.flush();}
//    //fprintf(stderr,"Thread: %lu  Document: %d  (out of %d)\n",(long)pthread_self(),d,trset->numdoc());
//    
//    float *WH=new float [dict->size()];
//    bool   *Hflags=new bool[topics];
//    
//    int M=trset->doclen(d); //vocabulary size of current documents with repetitions
//    
//    int N=M;  //document length
//    
//    //initialize H: we estimate one H for each document
//    for (int t=0; t<topics; t++) {H[(d % bucket) * topics + t]=1/(float)topics;Hflags[t]=true;}
//    
//    int iter=0;
//    
//    double LL=0;
//    float delta=0;
//    float maxdelta=1;
//    
//    while (iter < maxiter && maxdelta > deltathreshold){
//        
//        maxdelta=0;
//        iter++;
//        
//        //precompute denominator WH
//        for (int t=0; t<topics; t++)
//            if (Hflags[t] && H[(d % bucket) * topics + t] < topicthreshold){ Hflags[t]=false; H[(d % bucket) * topics + t]=0;}
//        
//        for (int i=0; i < M ; i++) {
//            WH[trset->docword(d,i)]=0; //initialized
//            for (int t=0; t<topics; t++){
//                if (Hflags[t])
//                    WH[trset->docword(d,i)]+=W[trset->docword(d,i)][t] * H[(d % bucket) * topics + t];
//            }
//            //LL-= log( WH[trset->docword(d,i)] );
//        }
//        
//        //cerr << "LL: " << LL << "\n";
//        
//        //UPDATE H
//        float totH=0;
//        for (int t=0; t<topics; t++) {
//            if (Hflags[t]){
//                float tmpH=0;
//                for (int i=0; i< M ; i++)
//                    tmpH+=(W[trset->docword(d,i)][t] * H[(d % bucket) * topics + t]/WH[trset->docword(d,i)]);
//                delta=abs(H[(d % bucket) * topics + t]-tmpH/N);
//                if (delta > maxdelta) maxdelta=delta;
//                H[(d % bucket) * topics + t]=tmpH/N;
//                totH+=H[(d % bucket) * topics + t]; //to check that sum is 1
//            }
//        }
//        
//        if(totH>UPPER_SINGLE_PRECISION_OF_1 || totH<LOWER_SINGLE_PRECISION_OF_1) {
//            cerr << "totH " << totH << "\n";
//            std::stringstream ss_msg;
//            ss_msg << "Total H is wrong; totH=" << totH << "\n";
//            exit_error(IRSTLM_ERROR_MODEL, ss_msg.str());
//        }
//        
//    }
//    //cerr << "Stopped at iteration " << iter << "\n";
//    
//    delete [] WH; delete [] Hflags;
//    
//    
//}
//
//
//
//int cswam::test(char *srctestfile, char *trgtestfile, char* modelfile, int maxit, char* alignfile){
//    
//    if (topicfeatfile) {mfstream out(topicfeatfile,ios::out);} //empty the file
//    //load existing model
//    initW(modelfile,0,0);
//    
//    //load existing model
//    trset=new doc(dict,testfile);
//    
//    bucket=BUCKET; //initialize the bucket size
//    maxiter=maxit; //set maximum number of iterations
//    
//    //use one vector H for all document
//    H=new float[topics*bucket]; memset(H,0,sizeof(float)*(long long)topics*bucket);
//    
//    threadpool thpool=thpool_init(threads);
//    task *t=new task[bucket];
//
//    
//    cerr << "start inference\n";
//    
//    for (long long d=0;d<trset->numdoc();d++){
//        
//        t[d % bucket].ctx=this; t[d % bucket].argv=(void *)d;
//        thpool_add_work(thpool, &plsa::single_inference_helper, (void *)&t[d % bucket]);
//        
//        if (((d % bucket) == (bucket-1)) || (d==(trset->numdoc()-1)) ){
//            //join all threads
//            thpool_wait(thpool);
//            
//            if ((d % bucket) != (bucket-1))
//                    bucket=trset->numdoc() % bucket; //last bucket at end of file
//            
//            if (topicfeatfile){
//                mfstream out(topicfeatfile,ios::out | ios::app);
//                
//                for (int b=0;b<bucket;b++){ //include the case of
//                    out << H[b * topics];
//                    for (int t=1; t<topics; t++) out << " "  << H[b * topics + t];
//                    out << "\n";
//                }
//            }
//            if (wordfeatfile){
//                cout << "from: " << d-bucket << " to: " << d-1 << "\n";
//                for (int b=0;b<bucket;b++) saveWordFeatures(wordfeatfile,d-bucket+b);
//            }
//            
//        }
//       
//        
//    }
//    
//    delete [] H; delete [] t;
//    delete trset;
//    return 1;
//}

