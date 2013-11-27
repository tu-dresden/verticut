#ifndef TIMER_S_H
#define TIMER_S_H
#include <iostream>
#include <map>
#include <string>
#include <sys/time.h>

class timer{
  protected:
    static std::map<std::string, double> table_; 
    struct timeval start_t_;
    struct timeval end_t_;
    std::string timer_id_;

  public:
    timer(const std::string& s){ 
      timer_id_ = s;
      gettimeofday(&start_t_, NULL);
    }

    ~timer(){ 
      gettimeofday(&end_t_, NULL);
      table_[timer_id_] += (double)((1000000 * (end_t_.tv_sec - start_t_.tv_sec)) + 
        (end_t_.tv_usec - start_t_.tv_usec)) / 1000000;
    }
  
    static void show_all_timings(){
      std::map<std::string, double>::iterator iter = table_.begin();
      
      std::cout<<"-------Timings-------"<<std::endl;
      for(; iter != table_.end(); ++iter)
        std::cout<<iter->first<<" : "<<iter->second<<" s"<<std::endl;
      std::cout<<"---------------------"<<std::endl; 
    }
};

#endif
