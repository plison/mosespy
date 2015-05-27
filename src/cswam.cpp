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
 
 *******************************************************************************/

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
	
cswam::cswam(char* sdfile,char *tdfile, char* w2vfile,bool usenull, bool normvect,bool scalevect,bool trainvar){
    
    //create dictionaries
    srcdict=new dictionary(NULL,100000); srcdict->generate(sdfile,true);
    trgdict=new dictionary(NULL,100000); trgdict->generate(tdfile,true);
    
    //make aware of oov word
    srcdict->encode(srcdict->OOV());
    trgdict->encode(trgdict->OOV());
    
    trgBoD = trgdict->encode(trgdict->BoD());  //codes for begin/end sentence markers
    trgEoD = trgdict->encode(trgdict->EoD());
    
    srcBoD = srcdict->encode(srcdict->BoD());  //codes for begin/end sentence markers
    srcEoD = srcdict->encode(srcdict->EoD());

    
    //load word2vec dictionary
    W2V=NULL; D=0;
    loadword2vec(w2vfile);

    //check consistency of word2vec with target vocabulary
    
    //actual model structure
    
    TM=NULL;
    
    //S=NULL;
    //M=NULL;
    //A=NULL;
    
    
    normalize_vectors=normvect;
    scale_vectors=scalevect;
    train_variances=trainvar;
    use_null_word=usenull;
    
    srandom(100); //ensure repicable generation of random numbers
    bucket=BUCKET;
    threads=1;
}

cswam::~cswam() {
    
    assert(A==NULL);
    
    if (TM){
        cerr << "Releasing memory of Translation Model\n";
        for (int e=0;e<trgdict->size();e++){
            for (int n=0;n<TM[e].n;n++){
                delete TM[e].G[n].M;delete TM[e].G[n].S;
            }
            delete [] TM[e].G; delete [] TM[e].W;
        }
        delete [] TM;
    }
    if (W2V){
        cerr << "Releasing memory of W2W\n";
        for (int f=0;f<srcdict->size();f++)
            if (W2V[f]!=NULL) delete [] W2V[f];
        delete [] W2V;
    }
    
    cerr << "Releasing memory of srcdict\n";
    delete srcdict;
    cerr << "Releasing memory of srcdict\n";
    delete trgdict;
    
    
}



void cswam::loadword2vec(char* fname){
    
    cerr << "Loading word2vec file " << fname << " ...\n";
    mfstream inp(fname,ios::in);
    
    long long w2vsize;
    inp >> w2vsize; cout << w2vsize << "\n";
    inp >> D ; cout << D  << "\n";
    
    int srcoov=srcdict->oovcode();
    
    W2V=new float* [srcdict->size()];
    for (int f=0;f<srcdict->size();f++) W2V[f]=NULL;
    
    char word[100]; float dummy; int f;
    
    for (long long i=0;i<w2vsize;i++){
        inp >> word;
        f=srcdict->encode(word);
        if (f != srcoov){
            W2V[f]=new float[D];
            for (int d=0;d<D;d++) inp >> W2V[f][d];
        }
        else //skip this word
            for (int d=0;d<D;d++) inp >> dummy;
        
        if (!(i % 100000)) cerr<< word << " ";
    }
    
    //looking for missing source words in w2v
    for ( f=0;f<srcdict->size();f++){
        if (W2V[f]==NULL && f!=srcBoD && f!=srcEoD) {
            cerr << "Missing src word in w2v: " << srcdict->decode(f) << "\n";
            W2V[f]=new float[D];
            for (int d=0;d<D;d++) W2V[f][d]=0;  //something better needed here
        }
    }
    
    
    cerr << "\n";
    
    
    if (normalize_vectors || scale_vectors){
        
        //normalized vector components
        float mean[D]; memset(mean,0,D*sizeof(float));
        float var[D]; memset(var,0,D*sizeof(float));
        
        //collect mean and variance statistics
        for ( f=0;f<srcdict->size();f++){
            for (int d=0;d<D;d++){
                mean[d]+=W2V[f][d];
                var[d]+=(W2V[f][d] * W2V[f][d]);
            }
        }
        //compute means and variances for each dimension
        for (int d=0;d<D;d++){
            mean[d]/=srcdict->size();
            var[d]=var[d]/srcdict->size() - (mean[d]*mean[d]);
            cerr << d << " mean: " << mean[d] << "  sd: " << sqrt(var[d]) << "\n";
        }
        
        
        if (normalize_vectors){
            cerr << "Shifting vectors\n";
            for (int f=0;f<srcdict->size();f++)
                for (int d=0;d<D;d++) W2V[f][d]=(W2V[f][d] - mean[d]);
        }
        if (normalize_vectors || scale_vectors){
            cerr << "Scaling vectors\n";
            for ( f=0;f<srcdict->size();f++)
                for (int d=0;d<D;d++) W2V[f][d]=W2V[f][d]/sqrt(var[d]);
            
        }
    }
    
    cerr << " ... done\n";
};


void cswam::initModel(char* modelfile){
    
    //test if model is readable
    bool model_available=false;
    FILE* f;if ((f=fopen(modelfile,"r"))!=NULL){fclose(f);model_available=true;}
    
    if (model_available) loadModel(modelfile,true); //we are in training mode!
    else{ //initialize model
        TM=new TransModel[trgdict->size()];
    
        for (int e=0; e<trgdict->size(); e++){
            TM[e].n=1;TM[e].G=new Gaussian [1];TM[e].W=new float[1];
            TM[e].G[0].M=new float [D];
            TM[e].G[0].S=new float [D];
            
            TM[e].G[0].eC=0;
            TM[e].G[0].mS=0;
            
            TM[e].W[0]=1;
            
            //initialize with w2v value if the same word is also in src
            int f=srcdict->encode(trgdict->decode(e));
            if (f!=srcdict->oovcode() && f!=srcBoD && f!=srcEoD){
                memcpy(TM[e].G[0].M,W2V[f],sizeof(float) * D);
                for (int d=0;d<D;d++) TM[e].G[0].S[d]=SSEED/4;
                cout << "initialize: " << srcdict->decode(f) << "\n";
            }else
                for (int d=0;d<D;d++){
                    TM[e].G[0].M[d]=0.0; //pick mean zero
                    TM[e].G[0].S[d]=SSEED; //take a wide standard deviation
                }
        }
    }
}

int cswam::saveModelTxt(char* fname){
    cerr << "Writing model into: " << fname << "\n";
    mfstream out(fname,ios::out);
    for (int e=0; e<trgdict->size(); e++){
        out << trgdict->decode(e) << " " << TM[e].n <<"\n";
        for (int n=0;n<TM[e].n;n++){
            out << TM[e].W[n] << "\n";
            for (int d=0;d<D;d++) out << TM[e].G[n].M[d] << " ";out << "\n";
            for (int d=0;d<D;d++) out << TM[e].G[n].S[d] << " ";out << "\n";
        }
    }
    return 1;
}

int cswam::saveModel(char* fname){
    cerr << "Saving model into: " << fname << " ...";
    mfstream out(fname,ios::out);
    out << "CSWAM " << D << "\n";
    trgdict->save(out);
    for (int e=0; e<trgdict->size(); e++){
        out.write((const char*)&TM[e].n,sizeof(int));
        out.write((const char*)TM[e].W,TM[e].n * sizeof(float));
        for (int n=0;n<TM[e].n;n++){
            out.write((const char*)TM[e].G[n].M,sizeof(float) * D);
            out.write((const char*)TM[e].G[n].S,sizeof(float) * D);
        }
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
    
    TM=new TransModel [trgdict->size()];
    
    cerr << "\nReading parameters .... ";
    for (int e=0; e<current_size; e++){
        inp.read((char *)&TM[e].n,sizeof(int));
        TM[e].W=new float[TM[e].n];
        inp.read((char *)TM[e].W,sizeof(float) * TM[e].n);
        TM[e].G=new Gaussian[TM[e].n];
        for (int n=0;n<TM[e].n;n++){
            TM[e].G[n].M=new float [D];
            TM[e].G[n].S=new float [D];
            inp.read((char *)TM[e].G[n].M,sizeof(float) * D);
            inp.read((char *)TM[e].G[n].S,sizeof(float) * D);
            TM[e].G[n].eC=0;TM[e].G[n].mS=0;
        }
    }
    inp.close();
    
    cerr << "\nInitializing " << trgdict->size()-current_size << " new entries .... ";
    for (int e=current_size; e<trgdict->size(); e++){
        TM[e].n=1;
        TM[e].W=new float[1];TM[e].W[0]=1.0;
        TM[e].G=new Gaussian[1];
        TM[e].G[0].M=new float [D];
        TM[e].G[0].S=new float [D];
        TM[e].G[0].eC=0;TM[e].G[0].mS=0;
        for (int d=0;d<D;d++){TM[e].G[0].M[d]=0.0;TM[e].G[0].S[d]=SSEED;}
    }
    
    cerr << "\nDone\n";
    return 1;
}

void cswam::initAlpha(){
    
    //install Alpha[s][i][j] to collect counts
    //allocate if empty
    
    if (A==NULL){
        assert(trgdata->numdoc()==srcdata->numdoc());
        A=new float ***[trgdata->numdoc()];
        for (int s=0;s<trgdata->numdoc();s++){
            A[s]=new float **[trgdata->doclen(s)];
            for (int i=0;i<trgdata->doclen(s);i++){
                A[s][i]=new float *[TM[trgdata->docword(s,i)].n];
                for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                    A[s][i][n]=new float [srcdata->doclen(s)];
            }
        }
    }
    //initialize
    for (int s=0;s<trgdata->numdoc();s++)
        for (int i=0;i<trgdata->doclen(s);i++)
            for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                memset(A[s][i][n],0,sizeof(float) * srcdata->doclen(s));
    
}

void cswam::freeAlpha(){
    
    if (A!=NULL){
        for (int s=0;s<trgdata->numdoc();s++){
            for (int i=0;i<trgdata->doclen(s);i++){
                for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                    delete [] A[s][i][n];
                delete [] A[s][i];
            }
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
    if (b<a) return a + logf(1 + expf(b-a));
    else return b + logf(1+ expf(a-b));
}


float cswam::LogGauss(const int dim,const float* x,const float *m, const float *s){
    
    static float log2pi=1.83787; //log(2 pi)
    float dist=0; float norm=0;

    for (int i=0;i<dim;i++){
        assert(s[i]>0);
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
    
    //reset likelihood
    localLL[s]=0;
    
    //compute denominator for each source-target pair
    for (int j=0;j<srclen;j++){
        //cout << "j: " << srcdict->decode(srcdata->docword(s,j)) << "\n";
        den=0;
        for (int i=0;i<trglen;i++)
            for (int n=0;n<TM[trgdata->docword(s,i)].n;n++){
                assert(TM[trgdata->docword(s,i)].W[n]>0); //weight zero must be prevented!!!
                A[s][i][n][j]=LogGauss(D, W2V[srcdata->docword(s,j)],
                                       TM[trgdata->docword(s,i)].G[n].M,
                                       TM[trgdata->docword(s,i)].G[n].S) + log(TM[trgdata->docword(s,i)].W[n]);
//                cerr <<  "rtarget: " << trgdict->decode(trgdata->docword(s,i))
//                <<   " n: " << n << " W=" << TM[trgdata->docword(s,i)].W[n]
//                << " A:" << A[s][i][n][j] << " den: " << den << "\n";
                if (i==0 && n==0) //den must be initialized
                    den=A[s][i][n][j];
                else
                    den=logsum(den,A[s][i][n][j]);
                //                    cerr << trgdict->decode(trgdata->docword(s,i)) << " n:" << n << "\n";
                //                    cerr << "DEN : " << den << "\n";
            }
        
        //update local likelihood
        localLL[s]+=den;
        
        for (int i=0;i<trglen;i++)
            for (int n=0;n<TM[trgdata->docword(s,i)].n;n++){
                
                assert(A[s][i][n][j]<= den);
//                if (!(A[s][i][n][j] <= den)){
//                    cerr << "\nsource: " << srcdict->decode(srcdata->docword(s,j)) << " trg: "<< trgdict->decode(trgdata->docword(s,i)) << " n: " <<  n << " A: " << A[s][i][n][j] <<  "\n";
//                    float locden=0;
//                    for (int li=0;li<trglen;li++){
//                        cerr << "target: " << trgdict->decode(trgdata->docword(s,li))
//                        << " size: " << TM[trgdata->docword(s,li)].n << "\n";
//                        for (int ln=0;ln<TM[trgdata->docword(s,li)].n;ln++){
//                            cerr <<  "n:" << ln << " W=" << TM[trgdata->docword(s,li)].W[ln]
//                            << " A:" << A[s][li][ln][j] << " den: " << den;
//                            if (TM[trgdata->docword(s,li)].W[ln]>0)
//                                cerr << " logA= "
//                                << LogGauss(D, W2V[srcdata->docword(s,j)],
//                                            TM[trgdata->docword(s,li)].G[ln].M,
//                                            TM[trgdata->docword(s,li)].G[ln].S) + log(TM[trgdata->docword(s,li)].W[ln]);
//                            cerr << "\n";
//                        }
//                    }
//                    exit(1);
//                }
                A[s][i][n][j]=expf(A[s][i][n][j]-den); // A is now a regular expected count
                
                if (A[s][i][n][j]<0.000000001) A[s][i][n][j]=0; //take mall risk of wrong normalization
                
                if (A[s][i][n][j]>0) TM[trgdata->docword(s,i)].G[n].eC++; //increase support set size
                
            }
    }
}

void cswam::maximization(void *argv){
    
    long long d=(long long) argv;
    
    if (!(d  % 10)) cerr <<".";
    //Maximization step: Mean;
    for (int s=0;s<srcdata->numdoc();s++)
        for (int i=0;i<trgdata->doclen(s);i++)
            for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                for (int j=0;j<srcdata->doclen(s);j++)
                    if (A[s][i][n][j]>0)
                        TM[trgdata->docword(s,i)].G[n].M[d]+=A[s][i][n][j] * W2V[srcdata->docword(s,j)][d];
    
    //second pass
    for (int e=0;e<trgdict->size();e++)
        for (int n=0;n<TM[e].n;n++)
            if (Den[e][n]>0)
                TM[e].G[n].M[d]/=Den[e][n]; //update the mean estimated
    
    if (train_variances){
        //Maximization step: Variance;
        
        for (int s=0;s<srcdata->numdoc();s++)
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                    for (int j=0;j<srcdata->doclen(s);j++)
                        if (A[s][i][n][j]>0)
                            TM[trgdata->docword(s,i)].G[n].S[d]+=
                            (A[s][i][n][j] *
                             (W2V[srcdata->docword(s,j)][d]-TM[trgdata->docword(s,i)].G[n].M[d]) *
                             (W2V[srcdata->docword(s,j)][d]-TM[trgdata->docword(s,i)].G[n].M[d])
                             );
        
        //second pass
        for (int e=0;e<trgdict->size();e++)
            for (int n=0;n<TM[e].n;n++)
                if (Den[e][n]>0){
                    TM[e].G[n].S[d]/=Den[e][n];
                    if (TM[e].G[n].S[d] < 0.01) TM[e].G[n].S[d]=0.01;
                }
    }
}


void cswam::expansion(void *argv){
    
    long long e=(long long) argv;
    for (int n=0;n<TM[e].n;n++){
        //get mean of variances
        float S=0; for (int d=0;d<D;d++) S+=TM[e].G[n].S[d]; S/=D;
        
        
        //show large support set and variances that do not reduce
        if (TM[e].G[n].eC >=4.0  && (S >= TM[e].G[n].mS && S > 1.0)){
            cerr << "\n" << trgdict->decode(e) << " n= " << n << " Counts: " << Den[e][n] << " mS: " << S << "\n";
            //cerr << "M: "; for (int d=0;d<D;d++) cerr << TM[e].G[n].M[d] << " "; cerr << "\n";
            //cerr << "S: "; for (int d=0;d<D;d++) cerr << TM[e].G[n].S[d] << " "; cerr << "\n";
            //expand: create new Gaussian after Gaussian n
            Gaussian *nG=new Gaussian[TM[e].n+1];
            float    *nW=new float[TM[e].n+1];
            memcpy((void *)nG,(const void *)TM[e].G, (n+1) * sizeof(Gaussian));
            memcpy((void *)nW,(const void *)TM[e].W, (n+1) * sizeof(float));
            if (n+1 < TM[e].n){
                memcpy((void *)&nG[n+2],(const void*)&TM[e].G[n+1],(TM[e].n-n-1) * sizeof(Gaussian));
                memcpy((void *)&nW[n+2],(const void*)&TM[e].W[n+1],(TM[e].n-n-1) * sizeof(float));
            }
            //initialize mean and variance vectors
            nG[n+1].M=new float[D];nG[n+1].S=new float[D];
            for (int d=0;d<D;d++){ //assign new means, keep old variances
                nG[n+1].S[d]=nG[n].S[d];
                nG[n+1].M[d]=nG[n].M[d]+sqrt(nG[n].S[d])/2;
                nG[n].M[d]=nG[n].M[d]-sqrt(nG[n].S[d])/2;
            }
            nG[n+1].eC=nG[n].eC;
            nG[n+1].mS=nG[n].mS=S;
            
            //initialize weight vectors uniformly over n and n+1
            nW[n+1]=nW[n]/2;nW[n]=nW[n]/2;
            
            //update TM[e] structure
            TM[e].n++;
            delete [] TM[e].G;TM[e].G=nG;
            delete [] TM[e].W; TM[e].W=nW;
            
            //we increment loop variable by 1
            n++;
        }else{
            TM[e].G[n].mS=S;
        }
        
    }
    
}

void cswam::contraction(void *argv){
    
    long long e=(long long) argv;

    for (int n=0;n<TM[e].n;n++){
        //show expected counts or variances that do not reduce
        if (TM[e].W[n] < 0.0001){ //eliminate this component
            assert(TM[e].n>1);
            cerr << "\n" << trgdict->decode(e) << " n= " << n << " Weight: " << TM[e].W[n] << "\n";
            //expand: create new Gaussian after Gaussian n
            Gaussian *nG=new Gaussian[TM[e].n-1];
            float    *nW=new float[TM[e].n-1];
            if (n>0){
                memcpy((void *)nG,(const void *)TM[e].G, n * sizeof(Gaussian));
                memcpy((void *)nW,(const void *)TM[e].W, n * sizeof(float));
            }
            if (n+1 < TM[e].n){
                memcpy((void *)&nG[n],(const void*)&TM[e].G[n+1],(TM[e].n-n-1) * sizeof(Gaussian));
                memcpy((void *)&nW[n],(const void*)&TM[e].W[n+1],(TM[e].n-n-1) * sizeof(float));
            }
            
            //don't need to normalized weights!
            
            //update TM[e] structure
            TM[e].n--;n--;
            delete [] TM[e].G;TM[e].G=nG;
            delete [] TM[e].W; TM[e].W=nW;
        }
    }

    for (int n=0;n<TM[e].n;n++) assert(TM[e].W[n] > 0.0001);
        

}

int cswam::train(char *srctrainfile, char*trgtrainfile,char *modelfile, int maxiter,int threads){
    
   
    initModel(modelfile);

    //Load training data

    srcdata=new doc(srcdict,srctrainfile);
    trgdata=new doc(trgdict,trgtrainfile,use_null_word); //use null word

   
    int iter=0;
    
    cerr << "Starting training";
    threadpool thpool=thpool_init(threads);
    int numtasks=trgdict->size()>trgdata->numdoc()?trgdict->size():trgdata->numdoc();
    task *t=new task[numtasks];
    assert(numtasks>D); //multi-threading also distributed over D
    
    threadpool thpool1=thpool_init(1);
    
    //support variable to compute model denominator
    Den=new float*[trgdict->size()];
    for (int e=0;e<trgdict->size();e++) Den[e]=new float[TM[e].n];
    
    //support variable to compute likelihood
    localLL=new float[srcdata->numdoc()];
    float LL;
    
    while (iter < maxiter){
        
        cerr << "\nIteration: " << ++iter <<  "\n";
        
        initAlpha();
        
        //reset support set size
        for (int e=0;e<trgdict->size();e++)
            for (int n=0;n<TM[e].n;n++)  TM[e].G[n].eC=0;
        
        
        cerr << "E-step: ";
        //compute expected counts in each single sentence
        for (long long  s=0;s<srcdata->numdoc();s++){
            //prepare and assign tasks to threads
            t[s].ctx=this; t[s].argv=(void *)s;
            thpool_add_work(thpool, &cswam::expected_counts_helper, (void *)&t[s]);
            
        }
        //join all threads
        thpool_wait(thpool);
        
        
        //Reset model before update
        for (int e=0;e <trgdict->size();e++)
            for (int n=0;n<TM[e].n;n++){
                memset(TM[e].G[n].M,0,D * sizeof (float));
                if (train_variances)
                    memset(TM[e].G[n].S,0,D * sizeof (float));
            }
        
        for (int e=0;e<trgdict->size();e++)
            memset(Den[e],0,TM[e].n * sizeof(float));
        
        LL=0; //compute LL of current model
        //compute normalization term for each target word
        for (int s=0;s<srcdata->numdoc();s++){
            LL+=localLL[s];
            for (int i=0;i<trgdata->doclen(s);i++)
                for (int n=0;n<TM[trgdata->docword(s,i)].n;n++)
                    for (int j=0;j<srcdata->doclen(s);j++)
                        Den[trgdata->docword(s,i)][n]+=A[s][i][n][j];
        }
        cerr << "LL = " << LL << "\n";
        
        
        cerr << "M-step: ";
        for (long long d=0;d<D;d++){
            t[d].ctx=this; t[d].argv=(void *)d;
            thpool_add_work(thpool, &cswam::maximization_helper, (void *)&t[d]);
        }
        
        //join all threads
        thpool_wait(thpool);

        //some checks of the models here
        for (int e=0;e<trgdict->size();e++){
            for (int n=0;n<TM[e].n;n++)
                if (!Den[e][n])
                    cerr << "Risk of degenerate model. Word: " << trgdict->decode(e) << " n: " << n << "\n";
            
//            if (trgdict->encode("bege")==e){
//                cerr << "bege " << " mS: " << TM[e].G[0].mS << " n: " << TM[e].n << " eC " << TM[e].G[0].eC << "\n";
//                cerr << "M:"; for (int d=0;d<10;d++) cerr << " " << TM[e].G[0].M[d]; cerr << "\n";
//                cerr << "S:"; for (int d=0;d<10;d++) cerr << " " << TM[e].G[0].S[d]; cerr << "\n";
//            }
        }
        
        //update the weight estimates: ne need of multithreading
        float totW;
        for (int e=0;e<trgdict->size();e++){
            totW=0;
            for (int n=0;n<TM[e].n;n++) totW+=Den[e][n];
            if (totW>0)
                for (int n=0;n<TM[e].n;n++) TM[e].W[n]=Den[e][n]/totW;
        }
        
        if (iter > 3){
            
            cerr << "\nExpansion step: ";
            freeAlpha(); //needs to be reallocated as models might change
            
            for (long long e=0;e<trgdict->size();e++){
                //check if to increase number of gaussians per target word
                t[e].ctx=this; t[e].argv=(void *)e;
                thpool_add_work(thpool1, &cswam::expansion_helper, (void *)&t[e]);
            }
            //join all threads
            thpool_wait(thpool1);
            
            cerr << "\nContraction step: ";
            for (long long e=0;e<trgdict->size();e++){
                //check if to decrease number of gaussians per target word
                t[e].ctx=this; t[e].argv=(void *)e;
                thpool_add_work(thpool1, &cswam::contraction_helper, (void *)&t[e]);
            }
            //join all threads
            thpool_wait(thpool1);
            
            
        }
        
       
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

    for (int e=0;e<trgdict->size();e++) delete [] Den[e]; delete [] Den;
    delete srcdata; delete trgdata;
    delete [] t; delete [] localLL;
    
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
    float score; float best_score;int best_i;

    for (int j=0;j<srclen;j++){
        
        best_score=-maxfloat;best_i=0;
        
        for (int i=0;i<trglen;i++)
            for (int n=0;n<TM[trgdata->docword(s,i)].n;n++){
                
                score=LogGauss(D,
                               W2V[srcdata->docword(s,j)],
                               TM[trgdata->docword(s,i)].G[n].M,
                               TM[trgdata->docword(s,i)].G[n].S)+log(TM[trgdata->docword(s,i)].W[n]);
                //  cout << "\t " << srcdict->decode(srcdata->docword(s,j)) << "  " << dist << "\n";
                //if (dist > -50) score=(float)exp(-dist)/norm;
                if (score > best_score){
                    best_score=score;
                    best_i=i;
                }
            }
        
        alignments[s % bucket][j]=best_i;
    }
}




int cswam::test(char *srctestfile, char *trgtestfile, char* modelfile, char* alignfile,int threads){
    
    {mfstream out(alignfile,ios::out);} //empty the file
    
    initModel(modelfile);
    
    //Load training data
    srcdata=new doc(srcdict,srctestfile);
    trgdata=new doc(trgdict,trgtestfile,use_null_word);
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
                    for (int j=0; j<srcdata->doclen(s-bucket+1+b); j++) out << " "  << j << "-" << alignments[b][j];
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

