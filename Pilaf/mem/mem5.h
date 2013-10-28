/*
** 2007 October 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
** 2012 September 18
**
** C++ class-icized by Christopher Mitchell
**
*************************************************************************
** This file contains the C functions that implement a memory
** allocation subsystem for use by SQLite. 
**
** This version of the memory allocation subsystem omits all
** use of malloc(). The SQLite user supplies a block of memory
** before calling sqlite3_initialize() from which allocations
** are made and returned by the xMalloc() and xRealloc() 
** implementations. Once sqlite3_initialize() has been called,
** the amount of memory available to SQLite is fixed and cannot
** be changed.
**
** This version of the memory allocation subsystem is included
** in the build only if SQLITE_ENABLE_MEMSYS5 is defined.
**
** $Id: c,v 1.19 2008/11/19 16:52:44 danielk1977 Exp $
*/

#include "string.h"
#include "assert.h"

/*
** This version of the memory allocator is used only when 
** SQLITE_ENABLE_MEMSYS5 is defined.
*/

/*
** A minimum allocation is an instance of the following structure.
** Larger allocations are an array of these structures where the
** size of the array is a power of 2.
*/

#define u64 long long unsigned int
#define u32 unsigned int
#define u16 unsigned short
#define u8  unsigned char
#define i64 long long int

#define SQLITE_ERROR 1
#define SQLITE_OK 0

// Toggle use of mutices on and off
//#define MEM_MUTICES

class PoolMalloc {

  struct Mem5Link {
    i64 next;       /* Index of next free chunk */
    i64 prev;       /* Index of previous free chunk */
  };
  typedef struct Mem5Link Mem5Link;

/*
** Maximum size of any allocation is ((1<<LOGMAX)*nAtom). Since
** nAtom is always at least 8, this is not really a practical
** limitation.
*/
#define LOGMAX 30

/*
** Masks used for aCtrl[] elements.
*/
#define CTRL_LOGSIZE  0x1f    /* Log2 Size of this block relative to POW2_MIN */
#define CTRL_FREE     0x20    /* True if not checked out */

  // These used to be the mem5 struct.
private:
  int nAtom;       /* Smallest possible allocation in bytes */
  i64 nBlock;      /* Number of nAtom sized blocks in zPool */
  u8 *zPool;
  
  /*
  ** Mutex to control access to the memory allocation subsystem.
  */
#ifdef MEM_MUTICES
  sqlite3_mutex *mutex;
#endif

  /*
  ** Performance statistics
  */
  u64 nAlloc;         /* Total number of calls to malloc */
  u64 totalAlloc;     /* Total of all malloc calls - includes internal frag */
  u64 totalExcess;    /* Total internal fragmentation */
  u32 currentOut;     /* Current checkout, including internal fragmentation */
  u32 currentCount;   /* Current number of distinct checkouts */
  u32 maxOut;         /* Maximum instantaneous currentOut */
  u32 maxCount;       /* Maximum instantaneous currentCount */
  u32 maxRequest;     /* Largest allocation (exclusive of internal frag) */
  
  /*
  ** Lists of free blocks of various sizes.
  */
  i64 aiFreelist[LOGMAX+1];

  /*
  ** Space for tracking which blocks are checked out and the size
  ** of each block.  One byte per block.
  */
  u8 *aCtrl;

#define MEM5LINK(idx) ((Mem5Link *)(&zPool[(idx)*nAtom]))

private:
/*
** Unlink the chunk at aPool[i] from list it is currently
** on.  It should be found on aiFreelist[iLogsize].
*/
void memsys5Unlink(i64 i, int iLogsize){
  i64 next, prev;
  assert( i>=0 && i<nBlock );
  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  assert( (aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  next = MEM5LINK(i)->next;
  prev = MEM5LINK(i)->prev;
  if( prev<0 ){
    aiFreelist[iLogsize] = next;
  }else{
    MEM5LINK(prev)->next = next;
  }
  if( next>=0 ){
    MEM5LINK(next)->prev = prev;
  }
}

/*
** Link the chunk at aPool[i] so that is on the iLogsize
** free list.
*/
void memsys5Link(i64 i, int iLogsize){
  i64 x;
#ifdef MEM_MUTICES
  assert( sqlite3_mutex_held(mutex) );
#endif
  assert( i>=0 && i<nBlock );
  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  assert( (aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  x = MEM5LINK(i)->next = aiFreelist[iLogsize];
  MEM5LINK(i)->prev = -1;
  if( x>=0 ){
    assert( x<nBlock );
    MEM5LINK(x)->prev = i;
  }
  aiFreelist[iLogsize] = i;
}

/*
** If the STATIC_MEM mutex is not already held, obtain it now. The mutex
** will already be held (obtained by code in malloc.c) if
** sqlite3GlobalConfig.bMemStat is true.
*/
void memsys5Enter(void){
#ifdef MEM_MUTICES
  if( sqlite3GlobalConfig.bMemstat==0 && mutex==0 ){
    mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mutex);
#endif
}
void memsys5Leave(void){
#ifdef MEM_MUTICES
  sqlite3_mutex_leave(mutex);
#endif
}

/*
** Return the size of an outstanding allocation, in bytes.  The
** size returned omits the 8-byte header overhead.  This only
** works for chunks that are currently checked out.
*/
u64 memsys5Size(void *p){
  u64 iSize = 0;
  if( p ){
    i64 i = ((u8 *)p-zPool)/nAtom;
    assert( i>=0 && i<nBlock );
    iSize = nAtom * (1 << (aCtrl[i]&CTRL_LOGSIZE));
  }
  return iSize;
}

/*
** Find the first entry on the freelist iLogsize.  Unlink that
** entry and return its index. 
*/
i64 memsys5UnlinkFirst(int iLogsize){
  i64 i;
  i64 iFirst;

  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  i = iFirst = aiFreelist[iLogsize];
  assert( iFirst>=0 );
  while( i>0 ){
    if( i<iFirst ) iFirst = i;
    i = MEM5LINK(i)->next;
  }
  memsys5Unlink(iFirst, iLogsize);
  return iFirst;
}

/*
** Return a block of memory of at least nBytes in size.
** Return NULL if unable.
*/
void *memsys5MallocUnsafe(size_t nByte){
  i64 i;           /* Index of an aPool[] slot */
  i64 iBin;        /* Index into aiFreelist[] */
  i64 iFullSz;     /* Size of allocation rounded up to power of 2 */
  int iLogsize;    /* Log2 of iFullSz/POW2_MIN */

  /* Keep track of the maximum allocation request.  Even unfulfilled
  ** requests are counted */
  if( (u32)nByte>maxRequest ){
    maxRequest = nByte;
  }

  /* Round nByte up to the next valid power of two */
  for(iFullSz=nAtom, iLogsize=0; iFullSz<nByte; iFullSz *= 2, iLogsize++){}

  /* Make sure aiFreelist[iLogsize] contains at least one free
  ** block.  If not, then split a block of the next larger power of
  ** two in order to create a new free block of size iLogsize.
  */
  for(iBin=iLogsize; aiFreelist[iBin]<0 && iBin<=LOGMAX; iBin++){}
  if( iBin>LOGMAX ) return 0;
  i = memsys5UnlinkFirst(iBin);
  while( iBin>iLogsize ){
    i64 newSize;

    iBin--;
    newSize = 1 << iBin;
    aCtrl[i+newSize] = CTRL_FREE | iBin;
    memsys5Link(i+newSize, iBin);
  }
  aCtrl[i] = iLogsize;

  /* Update allocator performance statistics. */
  nAlloc++;
  totalAlloc += iFullSz;
  totalExcess += iFullSz - nByte;
  currentCount++;
  currentOut += iFullSz;
  if( maxCount<currentCount ) maxCount = currentCount;
  if( maxOut<currentOut ) maxOut = currentOut;

  /* Return a pointer to the allocated memory. */
  return (void*)&zPool[i*nAtom];
}

/*
** Free an outstanding memory allocation.
*/
void memsys5FreeUnsafe(void *pOld){
  u32 size, iLogsize;
  i64 iBlock;             

  /* Set iBlock to the index of the block pointed to by pOld in 
  ** the array of nAtom byte blocks pointed to by zPool.
  */
  iBlock = ((u8 *)pOld-zPool)/nAtom;

  /* Check that the pointer pOld points to a valid, non-free block. */
  assert( iBlock>=0 && iBlock<nBlock );
  assert( ((u8 *)pOld-zPool)%nAtom==0 );
  assert( (aCtrl[iBlock] & CTRL_FREE)==0 );

  iLogsize = aCtrl[iBlock] & CTRL_LOGSIZE;
  size = 1<<iLogsize;
  assert( iBlock+size-1<(u32)nBlock );

  aCtrl[iBlock] |= CTRL_FREE;
  aCtrl[iBlock+size-1] |= CTRL_FREE;
  assert( currentCount>0 );
  assert( currentOut>=(size*nAtom) );
  currentCount--;
  currentOut -= size*nAtom;
  assert( currentOut>0 || currentCount==0 );
  assert( currentCount>0 || currentOut==0 );

  aCtrl[iBlock] = CTRL_FREE | iLogsize;
  while( iLogsize<LOGMAX ){
    i64 iBuddy;
    if( (iBlock>>iLogsize) & 1 ){
      iBuddy = iBlock - size;
    }else{
      iBuddy = iBlock + size;
    }
    assert( iBuddy>=0 );
    if( (iBuddy+(1<<iLogsize))>nBlock ) break;
    if( aCtrl[iBuddy]!=(CTRL_FREE | iLogsize) ) break;
    memsys5Unlink(iBuddy, iLogsize);
    iLogsize++;
    if( iBuddy<iBlock ){
      aCtrl[iBuddy] = CTRL_FREE | iLogsize;
      aCtrl[iBlock] = 0;
      iBlock = iBuddy;
    }else{
      aCtrl[iBlock] = CTRL_FREE | iLogsize;
      aCtrl[iBuddy] = 0;
    }
    size *= 2;
  }
  memsys5Link(iBlock, iLogsize);
}

public:

/*
** Allocate nBytes of memory
*/
void *memsys5Malloc(size_t nBytes){
  void *p = 0;
  if( nBytes>0 ){
    memsys5Enter();
    p = memsys5MallocUnsafe(nBytes);
    memsys5Leave();
  }
  return (void*)p; 
}

/*
** Free memory.
*/
void memsys5Free(void *pPrior){
  if( pPrior==0 ){
assert(0);
    return;
  }
  memsys5Enter();
  memsys5FreeUnsafe(pPrior);
  memsys5Leave();  
}

/*
** Change the size of an existing memory allocation
*/
void *memsys5Realloc(void *pPrior, u64 nBytes){
  u64 nOld;
  void *p;
  if( pPrior==0 ){
    return memsys5Malloc(nBytes);
  }
  if( nBytes<=0 ){
    memsys5Free(pPrior);
    return 0;
  }
  nOld = memsys5Size(pPrior);
  if( nBytes<=nOld ){
    return pPrior;
  }
  memsys5Enter();
  p = memsys5MallocUnsafe(nBytes);
  if( p ){
    memcpy(p, pPrior, nOld);
    memsys5FreeUnsafe(pPrior);
  }
  memsys5Leave();
  return p;
}

/*
** Round up a request size to the next valid allocation size.
*/
u64 memsys5Roundup(u64 n){
  u64 iFullSz;
  for(iFullSz=nAtom; iFullSz<n; iFullSz *= 2);
  return iFullSz;
}

int memsys5Log(u64 iValue){
  int iLog;
  for(iLog=0; ((u64)1<<iLog)<iValue; iLog++);
  return iLog;
}

/*
** Initialize this module.
*/
int memsys5Init(void* region, size_t size, int logminalloc) {
  i64 ii;
  i64 nByte = size; //sqlite3GlobalConfig.nHeap;
  u8 *zByte = (u8 *)region; //sqlite3GlobalConfig.pHeap;
  int nMinLog;                 /* Log of minimum allocation size in bytes*/
  i64 iOffset;

  if( !zByte ){
    return SQLITE_ERROR;
  }

  nMinLog = logminalloc; //memsys5Log(sqlite3GlobalConfig.mnReq);
  nAtom = (1<<nMinLog);
  while( (int)sizeof(Mem5Link)>nAtom ){
    nAtom = nAtom << 1;
  }

  nBlock = (nByte / (nAtom+sizeof(u8)));
  zPool = zByte;
  aCtrl = (u8 *)&zPool[nBlock*nAtom];

  for(ii=0; ii<=LOGMAX; ii++){
    aiFreelist[ii] = -1;
  }

  iOffset = 0;
  for(ii=LOGMAX; ii>=0; ii--){
    i64 nAlloc = (1<<ii);
    if( (iOffset+nAlloc)<=nBlock ){
      aCtrl[iOffset] = ii | CTRL_FREE;
      memsys5Link(iOffset, ii);
      iOffset += nAlloc;
    }
    assert(((u64)iOffset+(u64)nAlloc)>(u64)nBlock);
  }

  return SQLITE_OK;
}

/*
** Deinitialize this module.
*/
void memsys5Shutdown(void) {
  return;
}

/*
** Open the file indicated and write a log of all unfreed memory 
** allocations into that log.
*/
void sqlite3Memsys5Dump(const char *zFilename){
#ifdef SQLITE_DEBUG
  FILE *out;
  int i, j, n;
  int nMinLog;

  if( zFilename==0 || zFilename[0]==0 ){
    out = stdout;
  }else{
    out = fopen(zFilename, "w");
    if( out==0 ){
      fprintf(stderr, "** Unable to output memory debug output log: %s **\n",
                      zFilename);
      return;
    }
  }
  memsys5Enter();
  nMinLog = memsys5Log(nAtom);
  for(i=0; i<=LOGMAX && i+nMinLog<32; i++){
    for(n=0, j=aiFreelist[i]; j>=0; j = MEM5LINK(j)->next, n++){}
    fprintf(out, "freelist items of size %d: %d\n", nAtom << i, n);
  }
  fprintf(out, "nAlloc       = %llu\n", nAlloc);
  fprintf(out, "totalAlloc   = %llu\n", totalAlloc);
  fprintf(out, "totalExcess  = %llu\n", totalExcess);
  fprintf(out, "currentOut   = %u\n", currentOut);
  fprintf(out, "currentCount = %u\n", currentCount);
  fprintf(out, "maxOut       = %u\n", maxOut);
  fprintf(out, "maxCount     = %u\n", maxCount);
  fprintf(out, "maxRequest   = %u\n", maxRequest);
  memsys5Leave();
  if( out==stdout ){
    fflush(stdout);
  }else{
    fclose(out);
  }
#else
  // do nothing
#endif
}

};	//end PoolMalloc class
