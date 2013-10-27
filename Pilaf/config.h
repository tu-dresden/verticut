#include <string>
#include <vector>

struct server_info {
  std::string* host; 
  std::string* port;
};

class ConfigReader {
private:
  std::vector<struct server_info*> servers;
  bool ok;
  int at;

public:
  ConfigReader(std::string fname) {
    FILE* fh;

    servers.clear();
    ok = true;
    at = 0;

    // Try to open the config file
    if (NULL == (fh = fopen(fname.c_str(),"r"))) {
      ok = false;
      return;
    }

    // Fetch out items;
    while(!feof(fh)) {
      char host[1024], port[1024];
      if (2 < fscanf(fh,"%s %s\n",host,port)) {
        die("Malformed configuration entry");
      }

      struct server_info* tmpstruct = (struct server_info*)malloc(sizeof(struct server_info));
      if (NULL == tmpstruct) {
        ok = false;
        return;
      }
      tmpstruct->host = new std::string(host);
      tmpstruct->port = new std::string(port);
      servers.push_back(tmpstruct);
    }
    fclose(fh);

    return;
  }

  int get_count(void) { return servers.size(); }

  struct server_info* get_next(void) {
    return servers[at++];
  }

  void get_reset(void) {
    at = 0;
  }

  bool get_end(void) { return (at >= servers.size()); }
};
