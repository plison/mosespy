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
    dictionary *moddict; //model dictionary
    float **S;          //variance vector for target words
    float **M;          //mean vector for target words
    float ***A;         //expected count structure (threadsafe)
    
    
    //temporary file
    char Hfname[100]; //temporary and unique filename for H
    char *tmpdir;

    //private info shared among threads
    int threads;
    int bucket;
    struct task {
        void *ctx;
        void *argv;
    };

    
public:
    
    cswam(char* srcdatafile,char* trgdatafile, char* word2vecfile);
    ~cswam();
    
    void loadword2vec(char* fname);
    void initModel(char* fname);
    int saveModel(char* fname);
    int saveModelTxt(char* fname);
    int loadModel(char* fname,bool expand=false);
    
    void initAlpha();
    void freeAlpha();

    void expected_counts(void *argv);
    static void *expected_counts_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->expected_counts(t.argv);return NULL;
    };

    void aligner(void *argv);
    static void *aligner_helper(void *argv){
        task t=*(task *)argv;
        ((cswam *)t.ctx)->aligner(t.argv);return NULL;
    };
    
    int train(char *srctrainfile,char *trgtrainfile,char* modelfile, int maxiter,int threads=1);
//    int test(char *srctestfile, char* trgtestfile, char* modelfile,char* alignmentfile, int threads=1);
    
    
};

