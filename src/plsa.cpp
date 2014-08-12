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
#include "util.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "dictionary.h"
#include "n_gram.h"
#include "ngramtable.h"
#include "doc.h"
#include "cplsa.h"

using namespace std;

void print_help(int TypeFlag=0){
  std::cerr << std::endl << "plsa - performs probabilistic latent semantic analysis LM inference" << std::endl;
  std::cerr << std::endl << "USAGE:"  << std::endl;
	std::cerr << "       plsa -c=<text_collection> -d=<dictionary> -m=<model> -t=<topics> -it=<iter> [options]" << std::endl;
	std::cerr << "       plsa -c=<text_collection> -d=<dictionary> -b=<binary_collection> [options]" << std::endl;
	std::cerr << "       plsa -d=<dictionary> -m=<model> -t=<topics> -inf=<text> -f=<features> -it=<iterations> [options]" << std::endl;
  std::cerr << std::endl << "DESCRIPTION:" << std::endl;
  std::cerr << "       plsa is a tool for probabilistic latent semantic analysis" << std::endl;
  std::cerr << "       LM inference. It can be used to train a PLSA model, to binarize" << std::endl;
  std::cerr << "       a textual document collection to speed-up training or to" << std::endl;
  std::cerr << "       infer a full n-gram distribution from a model and a small text." << std::endl;
  std::cerr << std::endl << "OPTIONS:" << std::endl;
	
	
	FullPrintParams(TypeFlag, 0, 1, stderr);
	
  std::cerr << std::endl << "EXAMPLES:" << std::endl;
  std::cerr <<"       (1) plsa -c=<text_collection> -d=<dictionary> -m=<model> -t=<topics> -it=<iter>" << std::endl;
  std::cerr <<"           Train a PLSA model, <model>, from the text collection" << std::endl;
  std::cerr <<"           <text_collection> using the dictionary <dictionary>. The" << std::endl;
  std::cerr <<"           number of EM iterations is specified by <iter> and the" << std::endl;
  std::cerr <<"           number of topics is specified by <topics>." << std::endl;
  std::cerr <<"           The <text_collection> content must begin with the number of" << std::endl;
  std::cerr <<"           documents and documents should be separated with the </d> tag." << std::endl;
  std::cerr <<"           The begin document tag <d> is not considered." << std::endl;
  std::cerr <<"           Example of <text_collection> content:" << std::endl;
  std::cerr <<"           3" << std::endl;
  std::cerr <<"           <d> hello world ! </d>" << std::endl;
  std::cerr <<"           <d> good morning good afternoon </d>" << std::endl;
  std::cerr <<"           <d> welcome aboard </d>" << std::endl;
  std::cerr <<"       (2) plsa -c=<text_collection> -d=<dictionary> -b=<binary collection>" << std::endl;
  std::cerr <<"           Binarize a textual document collection to speed-up training (1)" << std::endl;
  std::cerr <<"       (3) plsa -d=<dictionary> -m=<model> -t=<topics> -inf=<text> -f=<features> -it=<iterations>" << std::endl;
  std::cerr <<"           Infer a full 1-gram distribution from a model and a small" << std::endl;
  std::cerr <<"           text. The 1-gram is saved in the feature file. The 1-gram" << std::endl;
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

int main(int argc, char **argv)
{
  char *dictfile=NULL;
  char *trainfile=NULL;
  char *adafile=NULL;
  char *featurefile=NULL;
  char *basefile=NULL;
  char *hfile=NULL;
  char *tmphfile=NULL;
  char *tfile=NULL;
  char *wfile=NULL;
  char *ctfile=NULL;
  char *txtfile=NULL;
  char *binfile=NULL;
	
  int numbins=1;  //number of document bins for parallel processing
  int topics=0;   //number of topics
  int st=0;       //special topic: first st dict words
  int it=0;       //number of EM iterations to run
  int tit=0;      //current EM iteration for multi-thread training
  bool help=false;
	
  DeclareParams((char*)
								
								"Dictionary", CMDSTRINGTYPE|CMDMSG, &dictfile, "dictionary file",
								"d", CMDSTRINGTYPE|CMDMSG, &dictfile, "dictionary file",
								
								"Binary", CMDSTRINGTYPE|CMDMSG, &binfile, "binary file",
								"b", CMDSTRINGTYPE|CMDMSG, &binfile, "binary file",
								
								"SplitData", CMDINTTYPE|CMDMSG, &numbins, "number of binary files (default 1)",
								"sd", CMDINTTYPE|CMDMSG, &numbins, "number of binary files (default 1)",
								
								"Collection", CMDSTRINGTYPE|CMDMSG, &trainfile, "text collection file",
								"c", CMDSTRINGTYPE|CMDMSG, &trainfile, "text collection file",
								
								"Model", CMDSTRINGTYPE|CMDMSG, &basefile, "model file",
								"m", CMDSTRINGTYPE|CMDMSG, &basefile, "model file",
								
								"HFile", CMDSTRINGTYPE, &tmphfile,
								"hf", CMDSTRINGTYPE, &tmphfile,
								
								"WFile", CMDSTRINGTYPE, &wfile,
								"wf", CMDSTRINGTYPE, &wfile,
								
								"TFile", CMDSTRINGTYPE, &tfile,
								"tf", CMDSTRINGTYPE, &tfile,
								
								"CombineTFile", CMDSTRINGTYPE, &ctfile,
								"ct", CMDSTRINGTYPE, &ctfile,
								
								"TxtFile", CMDSTRINGTYPE, &txtfile,
								"txt", CMDSTRINGTYPE, &txtfile,
								
								"Inference", CMDSTRINGTYPE, &adafile,
								"inf", CMDSTRINGTYPE, &adafile,
								
								"Features", CMDSTRINGTYPE, &featurefile,
								"f", CMDSTRINGTYPE, &featurefile,
								
								"Topics", CMDINTTYPE|CMDMSG, &topics, "number of topics; default is 0",
								"t", CMDINTTYPE|CMDMSG, &topics,"number of topics; default is 0",
								
								"SpecialTopic", CMDINTTYPE|CMDMSG, &st, "special topic for top st frequent words; default is 0",
								"st", CMDINTTYPE|CMDMSG, &st, "special topic for top st frequent words; default is 0",
								
								"Iterations", CMDINTTYPE|CMDMSG, &it, "number of EM iterations; default is 0",
								"it", CMDINTTYPE|CMDMSG, &it, "number of EM iterations; default is 0",
                
                                "ThreadIteration", CMDINTTYPE|CMDMSG, &tit, "thread iteration number; default is 0",
                                "tit", CMDINTTYPE|CMDMSG, &tit, "thread iteration number; default is 0",

								
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
	
  if (!dictfile) {
    usage();
		exit_error(IRSTLM_ERROR_DATA,"Missing dictionary file");
  };
    
    
  if ((trainfile && !binfile) && (!it || !topics || !basefile)) {
    usage();
	exit_error(IRSTLM_ERROR_DATA,"Missing training parameters");
  }
	
  if (ctfile && (!it || !topics || !basefile)) {
        usage();
		exit_error(IRSTLM_ERROR_DATA,"Missing recombination step parameters");
  }
    
  if (adafile && !(basefile || !featurefile || !it || !topics)) {
    usage();
		exit_error(IRSTLM_ERROR_DATA,"Missing inference parameters");
  }
    
	
  if (!tmphfile) {
    //set default value
    hfile=new char[4+1];
    strcpy(hfile,"hfff");
  } else {
    //set the value of the parameter
    hfile=new char[strlen(tmphfile)+1];
    strcpy(hfile,tmphfile);
  }
	
  dictionary dict(dictfile);
	
  
  dict.incflag(1);
  dict.encode(dict.BoD());
  dict.encode(dict.EoD());
  dict.incflag(0);
  dict.encode(dict.OOV());
  cout << "oovcode:"<< dict.oovcode() << "\n";
	
  cout << dict.size() << "\n";
	
  if (binfile) {
    cout << "opening collection\n";
    doc col(&dict,trainfile);
    col.open();
    if (numbins)
      col.save(binfile,numbins);
    else
      col.save(binfile);
    exit_error(IRSTLM_NO_ERROR);
  }
	
  system("rm -f hfff");
	
  plsa tc(&dict,topics,basefile,featurefile,hfile,wfile,tfile);
	
  if (ctfile) { //combine t
    tc.combineT(ctfile);
    if (txtfile) tc.saveWtxt(txtfile);
    tc.saveW(basefile);

    exit_error(IRSTLM_NO_ERROR);
  }
	
  if (trainfile){
    tc.train(trainfile,it,tit,.5,1,0.5,st);
    if (txtfile) tc.saveWtxt(txtfile);
  }
	
  if (adafile) {
    tc.loadW(basefile);
    tc.train(adafile,it,tit=0,.0);
  }
  if (strcmp(hfile,"hfff")==0)  system("rm -f hfff");
  delete []hfile;
	
	exit_error(IRSTLM_NO_ERROR);
}



