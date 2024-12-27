#ifndef COUNTCATEGORYTASK_H
#define COUNTCATEGORYTASK_H

#include "Task.h"

class CountCategoryTask : public Task {

public:
    CountCategoryTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~CountCategoryTask();

    void mainTaskThread() override;

    void addCategory(string category);
};

#endif // COUNTCATEGORYTASK_H