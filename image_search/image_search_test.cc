#include "image_search_client.h"
#include <iostream>
#include <list>
using namespace std;

int main(){
  image_search_client c("127.0.0.1", 9191);
  

  cout<<c.ping("hello world!")<<endl;
  
  list<pair<uint32_t, uint32_t> > res = c.search_image_by_id(0, 100, true);
  
  list<pair<uint32_t, uint32_t> >::iterator iter = res.begin();
  for(; iter != res.end(); iter++)
    cout<<iter->first<<" :: "<<iter->second<<endl;

  return 0;
}
