#ifndef COUNTTASK_H
#define COUNTTASK_H

#include "Task.h"

class CountTask : public Task {

    mutex wordCountMutex;
    unordered_map<string, int> wordCount;

public:
    CountTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~CountTask();

    void mainTaskThread() override;

    void addWord(string word);
    void printWordCount();


    
};

#endif // COUNTTASK_H