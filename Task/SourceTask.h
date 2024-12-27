#ifndef SOURCETASK_H
#define SOURCETASK_H

#include "Task.h"

class SourceTask : public Task {

    string srcFilePath, hydfsSrcFile;
public:
    SourceTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~SourceTask();

    void mainTaskThread() override;
    void messageProcessorThread();

    void startTask() override;
};

#endif // SOURCETASK_H