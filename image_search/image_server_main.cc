#include <iostream>
#include "image_search_server.h"

int main(){
  image_search_server server;
  server.init_workers();
  server.instance.listen("0.0.0.0", 9191);
  server.instance.run(10);

  return 0;
}
