// $Id: util.h 363 2010-02-22 15:02:45Z mfederico $

#ifndef IRSTLM_UTIL_H
#define IRSTLM_UTIL_H

#include <string>
#include <fstream>
#include "gzfilebuf.h"
#include "n_gram.h"

#define UNUSED(x) { (void) x; }

#define DEBUG
#define _DEBUG_LEVEL 2

/** trace macros **/
#ifdef TRACE_ENABLE
#define TRACE_ERR(str) { std::cerr << str; }
#else
#define TRACE_ERR(str) { }
#endif

/** verbose macros **/
#ifdef DEBUG
#define VERBOSE(level,str) { if (_DEBUG_LEVEL){  if (_DEBUG_LEVEL >= level) { TRACE_ERR("DEBUG_LEVEL:" <<_DEBUG_LEVEL << " "); TRACE_ERR(str); }  } }
#else
#define VERBOSE(level,str) { UNUSED(level); UNUSED(str)); }
#endif

#define IFVERBOSE(level) if (_DEBUG_LEVEL) if (_DEBUG_LEVEL >= level)


#define LMTMAXLEV  20
#define MAX_LINE  1024

std::string gettempfolder();
void createtempfile(std::ofstream  &fileStream, std::string &filePath, std::ios_base::openmode flags);
void removefile(const std::string &filePath);

class inputfilestream : public std::istream
{
protected:
	std::streambuf *m_streambuf;
  bool _good;
public:
  
	inputfilestream(const std::string &filePath);
	~inputfilestream();
  bool good(){return _good;}
	void close();
};

void *MMap(int	fd, int	access, off_t	offset, size_t	len, off_t	*gap);
int Munmap(void	*p,size_t	len,int	sync);


// A couple of utilities to measure access time
void ResetUserTime();
void PrintUserTime(const std::string &message);
double GetUserTime();


int parseWords(char *, const char **, int);
int parseline(istream& inp, int Order,ngram& ng,float& prob,float& bow);

#endif

