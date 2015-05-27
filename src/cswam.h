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

typedef struct{
    
    float*  M;  //mean vectors
    float*  S;  //variance vectors
    //training support items
    float   eC; //support set size
    float   mS; //mean variance
    
} Gaussian;

typedef struct{
    int      n;  //number of Gaussians
    float    *W; //weight vector
    Gaussian *G; //Gaussians
} TransModel;

class cswam {
    
    //data
    dictionary* srcdict; //source dictionary
    dictionary* trgdict; //target dictionary
    doc* srcdata;   //source training data
    doc* trgdata;   //target trainign data
    
    //word2vec
    float     **W2V;   //vector for each source word!
    int       D;       //dimension of vector space
    
    //model
    TransModel *TM;
    
    
    //settings
    bool normalize_vectors;
    bool scale_vectors;
    bool train_variances;
    bool use_null_word;

    //private info shared among threads
    int trgBoD;        //code of segment begin in target dict
    int trgEoD;        //code of segment end in target dict
    int srcBoD;        //code of segment begin in src dict
    int srcEoD;        //code of segment end in src dict

    float ****A;       //expected counts
    float **Den;       //alignment probs
    float *localLL;    //local log-likelihood
    int **alignments;  //word alignment info
    int threads;       //number of threads
    int bucket;        //size of bucket

    struct task {      //basic task info to run task
        void *ctx;
        void *argv;
    };

    
public:
    
    cswam(char* srcdatafile,char* trgdatafile, char* word2vecfile,bool usenull,bool normv2w,bool scalew2v,bool trainvar);
    ~cswam();
    
    void loadword2vec(char* fname);
    void initModel(char* fname);
    int saveModel(char* fname);
    int saveModelTxt(char* fname);
    int loadModel(char* fname,bool expand=false);
    
    void initAlpha();
    void freeAlpha();
    
    float LogGauss(const int dim,const float* x,const float *m, const float *s);
        
    void expected_counts(void *argv);
    static void *expected_counts_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->expected_counts(t.argv);return NULL;
    };

    void maximization(void *argv);
    static void *maximization_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->maximization(t.argv);return NULL;
    };

    void expansion(void *argv);
    static void *expansion_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->expansion(t.argv);return NULL;
    };
    
    void contraction(void *argv);
    static void *contraction_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->contraction(t.argv);return NULL;
    };
    
    int train(char *srctrainfile,char *trgtrainfile,char* modelfile, int maxiter,int threads=1);
    
    void aligner(void *argv);
    static void *aligner_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->aligner(t.argv);return NULL;
    };
    

    int test(char *srctestfile, char* trgtestfile, char* modelfile,char* alignmentfile, int threads=1);
    
};

