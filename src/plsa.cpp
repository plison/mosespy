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

using namespace std;

#include <iostream>
#include "cmd.h"
#include "mfstream.h"
#include "mempool.h"
#include "htable.h"
#include "dictionary.h"
#include "n_gram.h"
#include "ngramtable.h"
#include "doc.h"
#include "cplsa.h"

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
	exit(1);
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
	
  int binsize=0;
  int topics=0;  //number of topics
  int st=0;      //special topic: first st dict words
  int it=0;
  bool help=false;
	
  DeclareParams((char*)
								
								"Dictionary", CMDSTRINGTYPE|CMDMSG, &dictfile, "dictionary file",
								"d", CMDSTRINGTYPE|CMDMSG, &dictfile, "dictionary file",
								
								"Binary", CMDSTRINGTYPE|CMDMSG, &binfile, "binary file",
								"b", CMDSTRINGTYPE|CMDMSG, &binfile, "binary file",
								
								"SplitData", CMDINTTYPE|CMDMSG, &binsize, "size of binary file; default is unlimited",
								"sd", CMDINTTYPE|CMDMSG, &binsize, "size of binary file; default is unlimited",
								
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
								
								"SpecialTopic", CMDINTTYPE|CMDMSG, &st, "special topic: first dictionary words; default is 0",
								"st", CMDINTTYPE|CMDMSG, &st, "special topic: first dictionary words; default is 0",
								
								"Iterations", CMDINTTYPE|CMDMSG, &it, "number of EM iterations; default is 0",
								"it", CMDINTTYPE|CMDMSG, &it, "number of EM iterations; default is 0",
								
								"Help", CMDBOOLTYPE|CMDMSG, &help, "print this help",
								"h", CMDBOOLTYPE|CMDMSG, &help, "print this help",
								
								(char *)NULL
								);
	
	if (argc == 1){
		usage();
	}
	
  GetParams(&argc, &argv, (char*) NULL);
	
	if (help){
		usage();
	}
	
  if (!dictfile) {
    usage("Missing parameters dictionary");
  };
	
  if (!adafile & (!trainfile || !binfile) && (!trainfile || !it || !topics || !basefile)) {
    usage("Missing parameters for training");
  }
	
  if ((!trainfile && basefile) && (!featurefile || !adafile || !it || !topics)) {
    usage("Missing parameters for adapting");
  }
	
  if ((adafile) && (!featurefile)) {
    usage("Missing parameters for adapting 2");
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
	
  cout << dict.size() << "\n";
  dict.incflag(1);
  dict.encode(dict.BoD());
  dict.encode(dict.EoD());
  dict.incflag(0);
  if (dict.oovcode()==-1) {
    dict.oovcode(dict.encode(dict.OOV()));
  }
	
  cout << dict.size() << "\n";
	
  if (binfile) {
    cout << "opening collection\n";
    doc col(&dict,trainfile);
    col.open();
    if (binsize)
      col.save(binfile,binsize);
    else
      col.save(binfile);
    exit(1);
  }
	
  system("rm -f hfff");
	
  plsa tc(&dict,topics,basefile,featurefile,hfile,wfile,tfile);
	
  if (ctfile) { //combine t
    tc.combineT(ctfile);
    tc.saveW(basefile);
    exit(1);
  }
	
  if (trainfile) {
    tc.train(trainfile,it,.5,1,0.5,st);
    if (txtfile) tc.saveWtxt(txtfile);
  }
	
  if (adafile) {
    tc.loadW(basefile);
    tc.train(adafile,it,.0);
  }
  if (strcmp(hfile,"hfff")==0)  system("rm -f hfff");
  delete hfile;
	
  exit(1);
}



