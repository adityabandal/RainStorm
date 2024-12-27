#include "FilterSignPostTask.h"
#include <fstream>

FilterSignPostTask::FilterSignPostTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs, string param) : Task(scheduleMessage, manager, dfs) {
    setParam(param);
    cout << "FilterSignPost task created" << endl;
}

FilterSignPostTask::~FilterSignPostTask() {
    cout << "FilterSignPost task destroyed" << endl;
}

void FilterSignPostTask::mainTaskThread() {
    RainStormMessage message;
    while (taskRunning) {
        if (getMessageFromQueue(message)) {
            if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                // cout<<"Received ack task: "<<message.getTaskId()<<get<0>(message.getTuple())<<endl;
                addAckToJournalBuffer(get<0>(message.getTuple()));
            } else if (message.getType() == RainStormMessageType::INPUT) {
                
                string pattern = getParam();
                // cout << "Filtering pattern: " << pattern << endl;

                // 1. Discard if already processed
                if (isInputProcessed(get<0>(message.getTuple()))) {
                    // cout<<"Duplicate i/p task: "<<message.getTaskId()<<get<0>(message.getTuple())<<endl;
                    ackInputProcessed(message);
                } else {
                    // 2. Process the input
                    string line = get<2>(message.getTuple());
                    vector<string> columns = extractColumns(line);
                    vector<Tuple> processedInputs;
                    if (columns.size() >= 9 && columns[6] == pattern) {
                        Tuple outputTuple = make_tuple(get<0>(message.getTuple()), columns[8], "1");
                        // printOutput(outputTuple);
                        processedInputs.push_back(outputTuple);
                    }

                    // 3. Log the inputs, outputs, and state
                    // 4. Ack previous task
                    addToJournalBuffer(get<0>(message.getTuple()), processedInputs, message);

                    // 5. send to next task
                    for(Tuple outputTuple : processedInputs) {
                        sendOutputToNextTask(outputTuple);
                    }
                }
            } else {
                cout << "NOT Expected this message here - filter" << endl;
            }
        }
    }
    
}

// void FilterSignPostTask::printOutput(Tuple outputTuple) {
//     cout << "FilterSignPost Output: " << get<0>(outputTuple) << " - " << get<1>(outputTuple) << " - " << get<2>(outputTuple) << endl;
// }