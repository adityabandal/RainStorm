#include "CountTask.h"

CountTask::CountTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) : Task(scheduleMessage, manager, dfs) {
    cout << "Count task created" << endl;
}

CountTask::~CountTask() {
    cout << "Count task destroyed" << endl;
}

void CountTask::mainTaskThread() {
    RainStormMessage message;
    while(taskRunning) {
        if(getMessageFromQueue(message)) {
            // cout << "Count task received message: " << get<0>(message.getTuple()) << endl;

            if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                // Nothing to do here, count is the last task
            } else if (message.getType() == RainStormMessageType::INPUT) {

                // 1. Discard if alread processe
                if(isInputProcessed(get<0>(message.getTuple()))) {
                    ackInputProcessed(message);
                    printWordCount();
                } else {
                    // 2. Process the input

                    // 3. log the inputs, outputs and state

                    // 4. Ack previous task
                    markInputProcessed(get<0>(message.getTuple()));
                    ackInputProcessed(message);

                    // 5. Send output to next task and buffer
                    addWord(get<1>(message.getTuple()));
                    printWordCount();

                }
            } else {
                cout<<"NOT Expected this message here - count"<<endl;
            }
        }
    }
}

void CountTask::addWord(string word) {
    lock_guard<mutex> lock(wordCountMutex);
    if(wordCount.find(word) == wordCount.end()) {
        wordCount[word] = 1;
    } else {
        wordCount[word]++;
    }
}

void CountTask::printWordCount() {
    lock_guard<mutex> lock(wordCountMutex);
    cout << "Word count: " << endl;
    for(auto& p : wordCount) {
        cout << p.first << " : " << p.second << endl;
    }
}