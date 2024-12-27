#include "SelectTask.h"
#include "fstream"

SelectTask::SelectTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) : Task(scheduleMessage, manager, dfs) {
    cout << "Select task created" << endl;
    resultFile = scheduleMessage.getDestFileName();
}

SelectTask::~SelectTask() {
    cout << "Select task destroyed" << endl;
}

void SelectTask::mainTaskThread() {
    RainStormMessage message;
    while (taskRunning) {
        if (getMessageFromQueue(message)) {
            if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                // Nothing to do here, select is the last task
            } else if (message.getType() == RainStormMessageType::INPUT) {

                // 1. Discard if already processed
                if (isInputProcessed(get<0>(message.getTuple()))) {
                    ackInputProcessed(message);
                } else {
                    
                    // 2. Process the input
                    string line = get<2>(message.getTuple());
                    vector<string> columns = extractColumns(line);
                    Tuple outputTuple = make_tuple(get<0>(message.getTuple()), columns[2], columns[3]);
                    
                    
                    // 3. Log the inputs, outputs, and state
                    // 4. Ack previous task
                    addToJournalBuffer(get<0>(message.getTuple()), {outputTuple}, message);
                    
                    // 5. write buffer
                    addOutputToBuffer(outputTuple, getCurrentTSinEpoch());
                }
            } else {
                cout << "NOT Expected this message here - select" << endl;
            }
        }
    }
}
