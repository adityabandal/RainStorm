#ifndef FILTERSIGNPOSTTASK_H
#define FILTERSIGNPOSTTASK_H

#include "Task.h"

class FilterSignPostTask : public Task {
public:
    FilterSignPostTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs, string param);
    ~FilterSignPostTask();
    void mainTaskThread() override;
    // void printOutput(Tuple outputTuple);
    
    const std::string& getParam() const {return param;}

    void setParam(const std::string& newParam) {param = newParam;}

private:
    std::string param;
};

#endif // FILTERSIGNPOSTTASK_H