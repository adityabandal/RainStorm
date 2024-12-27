#ifndef SPLIT_TASK_H
#define SPLIT_TASK_H

#include "Task.h"

class SplitTask : public Task {
public:
    SplitTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~SplitTask();

    void mainTaskThread() override;
    vector<string> splitLine(string line);
};

#endif // SPLIT_TASK_H