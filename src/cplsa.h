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


class plsa {
    dictionary* dict; //dictionary
    int topics;       //number of topics
    doc* trset;       //training/inference set

    double **T;       //support matrix (keep double precision here!)
    
    float **W;       //word - topic matrix
    float *H;        //document-topic: matrix (memory mapped)
    
    char Hfname[100]; //temporary and unique filename for H
    char *tmpdir;
    bool memorymap;   //use or not memory mapping
    struct task {
        void *ctx;
        void *argv;
    };
    
public:
   
    
    plsa(dictionary* dict,int topics,char* workdir,bool mm);
    ~plsa();
    
    int saveW(char* fname);
    int saveWtxt(char* fname,int tw=10);
    int loadW(char* fname);
    
    int initW(char* modelfile, float noise,int spectopic); int freeW();
    int initH();int freeH();
    int initT();int freeT();

    void expected_counts(void *argv);

    static void *expected_counts_helper(void *argv){
        task t=*(task *)argv;
        ((plsa *)t.ctx)->expected_counts(t.argv);return NULL;};
    
    
    int train(char *trainfile,char* modelfile, int maxiter,int threads, float noiseW,int spectopic=0);
    int inference(char *trainfile, char* modelfile, int maxiter, char* topicfeatfile,char* wordfeatfile);
    
    int saveWordFeatures(char* fname);
    
};

