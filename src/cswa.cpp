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


#include <iostream>
#include "cmd.h"
#include <pthread.h>
#include "thpool.h"
#include "util.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "dictionary.h"
#include "n_gram.h"
#include "ngramtable.h"
#include "doc.h"
#include "cswam.h"

using namespace std;

void print_help(int TypeFlag=0){
    std::cerr << std::endl << "cswa -  continuous space word alignment model" << std::endl;
    std::cerr << std::endl << "USAGE:"  << std::endl;
    std::cerr << "       cswa -str=<text> -ttr=<text> -w2v=word2vectmodel -o=model-file [options]" << std::endl;
     std::cerr << std::endl;
}

void usage(const char *msg = 0)
{
  if (msg){
    std::cerr << msg << std::endl;
	}
  else{
		print_help();
	}
}

int main(int argc, char **argv){

    char *srcdatafile=NULL;
    char *trgdatafile=NULL;
    
    char *w2vfile=NULL;
    char *modelfile=NULL;
    char *alignfile=NULL;

    bool forcemodel=false;
    
    int iterations=0;       //number of EM iterations to run
    int threads=1;          //current EM iteration for multi-thread training
    bool help=false;
    int prunethreshold=3;
    bool trainvar=true;
    bool normvectors=false;
    bool scalevectors=false;
    bool usenullword=true;
    
    DeclareParams((char*)
                  
                  
                  "SrcData", CMDSTRINGTYPE|CMDMSG, &srcdatafile, "<fname> : source text collection ",
                  "sd", CMDSTRINGTYPE|CMDMSG, &srcdatafile, "<fname> : source text collection ",
                  
                  "TrgData", CMDSTRINGTYPE|CMDMSG, &trgdatafile, "<fname> : target text collection ",
                  "td", CMDSTRINGTYPE|CMDMSG, &trgdatafile, "<fname> : target text collection ",
                  
                  "Word2Vec", CMDSTRINGTYPE|CMDMSG, &w2vfile, "<fname> : word2vec file ",
                  "w2v", CMDSTRINGTYPE|CMDMSG, &w2vfile, "<fname> : word2vec file ",
                  
                  "Model", CMDSTRINGTYPE|CMDMSG, &modelfile, "<fname> : model file",
                  "m", CMDSTRINGTYPE|CMDMSG, &modelfile, "<fname> : model model file",
                  
                  "Iterations", CMDINTTYPE|CMDMSG, &iterations, "<count> : training iterations",
                  "it", CMDINTTYPE|CMDMSG, &iterations, "<count> : training iterations",
                  
                  "Alignments", CMDSTRINGTYPE|CMDMSG, &alignfile, "<fname> : output alignment file",
                  "al", CMDSTRINGTYPE|CMDMSG, &alignfile, "<fname> : output alignment file",
                  
                  "UseNullWord", CMDBOOLTYPE|CMDMSG, &usenullword, "<bool>: use null word (default true)",
                  "unw", CMDBOOLTYPE|CMDMSG, &usenullword, "<bool>: use null word (default true)",
                  
                  "Threads", CMDINTTYPE|CMDMSG, &threads, "<count>: number of threads (default 2)",
                  "th", CMDINTTYPE|CMDMSG, &threads, "<count>: number of threads (default 2)",
                  
                  "ForceModel", CMDBOOLTYPE|CMDMSG, &forcemodel, "<bool>: force to use existing model for training",
                  "fm", CMDBOOLTYPE|CMDMSG, &forcemodel, "<bool>: force to use existing model for training",
                  
                  "TrainVariances", CMDBOOLTYPE|CMDMSG, &trainvar, "<bool>: train variances (default true)",
                  "tv", CMDBOOLTYPE|CMDMSG, &trainvar, "<bool>: train variances (default true)",
                
                  "NormalizeVectors", CMDBOOLTYPE|CMDMSG, &normvectors, "<bool>: normalize vectors  (default false)",
                  "nv", CMDBOOLTYPE|CMDMSG, &normvectors, "<bool>: normalize vectors  (default false)",
                  
                  "ScaleVectors", CMDBOOLTYPE|CMDMSG, &scalevectors, "<bool>: scale vectors  (default false)",
                  "sv", CMDBOOLTYPE|CMDMSG, &scalevectors, "<bool>: scale vectors  (default false)",
                  
                  "Help", CMDBOOLTYPE|CMDMSG, &help, "print this help",
                  "h", CMDBOOLTYPE|CMDMSG, &help, "print this help",
                  
                  (char *)NULL
                  );
    
    if (argc == 1){
        usage();
        exit_error(IRSTLM_NO_ERROR);
    }
    
    GetParams(&argc, &argv, (char*) NULL);
    
    if (help){
        usage();
        exit_error(IRSTLM_NO_ERROR);
    }
    
    
    if (!srcdatafile || !trgdatafile || !w2vfile || !modelfile ) {
        usage();
        exit_error(IRSTLM_ERROR_DATA,"Missing parameters");
    }
    
     //check if model is readable
    bool testmodel=false;
    FILE* f;if ((f=fopen(modelfile,"r"))!=NULL){fclose(f);testmodel=true;}
    
    if (iterations && testmodel && !forcemodel)
          exit_error(IRSTLM_ERROR_DATA,"Use -ForceModel=y option to update an existing model.");
    
    cswam *model=new cswam(srcdatafile,trgdatafile,w2vfile,usenullword,normvectors,scalevectors,trainvar);
    
    if (iterations)
        model->train(srcdatafile,trgdatafile,modelfile,iterations,threads);
    
    if (alignfile)
        model->test(srcdatafile,trgdatafile,modelfile,alignfile,threads);
    
    delete model;
    
    exit_error(IRSTLM_NO_ERROR);
}



