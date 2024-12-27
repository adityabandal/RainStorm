#include "CountCategoryTask.h"
#include <fstream>

CountCategoryTask::CountCategoryTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) : Task(scheduleMessage, manager, dfs) {
    cout << "CountCategory task created" << endl;
}

CountCategoryTask::~CountCategoryTask() {
    cout << "CountCategory task destroyed" << endl;
}

void CountCategoryTask::mainTaskThread() {
    RainStormMessage message;
    while (taskRunning) {
        if (getMessageFromQueue(message)) {
            if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                // cout<<"Received ack task: "<<message.getTaskId()<<get<0>(message.getTuple())<<endl;

                // Nothing to do here, category count is the last task
            } else if (message.getType() == RainStormMessageType::INPUT) {
                
                // 1. Discard if already processed
                if (isInputProcessed(get<0>(message.getTuple()))) {
                    // cout<<"Duplicate i/p task: "<<message.getTaskId()<<get<0>(message.getTuple())<<endl;
                    ackInputProcessed(message);
                } else {
                    // 2. Process the input
                    string category = get<1>(message.getTuple());
                    // addToStateMap(category);

                    // 3. Log the inputs, outputs, and state
                    // 4. Ack previous task
                    Tuple changeTuple = make_tuple(get<0>(message.getTuple()), category, "1");
                    addToJournalBuffer(get<0>(message.getTuple()), {changeTuple}, message);
                    
                    // 5. treat state as write buffer, no need to add anything
                    
                }
            } else {
                cout << "NOT Expected this message here - count" << endl;
            }
        }
    }
}

// void CountCategoryTask::addCategory(string category) {
//     lock_guard<mutex> lock(categoryCountMutex);
//     if (categoryCount.find(category) == categoryCount.end()) {
//         categoryCount[category] = 1;
//     } else {
//         categoryCount[category]++;
//     }
// }