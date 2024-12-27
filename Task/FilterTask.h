#ifndef FILTERTASK_H
#define FILTERTASK_H

#include "Task.h"

class FilterTask : public Task {
public:
    FilterTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs, string param);
    ~FilterTask();
    void mainTaskThread() override;
    void printOutput(Tuple outputTuple);
    
    const std::string& getParam() const {return param;}

    void setParam(const std::string& newParam) {param = newParam;}

private:
    std::string param;
};

#endif // FILTERTASK_H