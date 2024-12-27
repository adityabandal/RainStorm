#ifndef SELECTTASK_H
#define SELECTTASK_H

#include "Task.h"

class SelectTask : public Task {
public:
    SelectTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~SelectTask();
    void mainTaskThread() override;

private:
    string resultFile;
};

#endif // SELECTTASK_H