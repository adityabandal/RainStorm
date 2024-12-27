#include "FilterTask.h"
#include <fstream>

FilterTask::FilterTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs, string param) : Task(scheduleMessage, manager, dfs) {
    setParam(param);
    cout << "Filter task created" << endl;
}

FilterTask::~FilterTask() {
    cout << "Filter task destroyed" << endl;
}

void FilterTask::mainTaskThread() {
    RainStormMessage message;
    while(taskRunning) {
        if(getMessageFromQueue(message)) {
            if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                // cout<<"Received ack message for task: "<<message.getTaskId()<<endl;
                addAckToJournalBuffer(get<0>(message.getTuple()));
            } else if (message.getType() == RainStormMessageType::INPUT) {
                
                string pattern = getParam();
                // cout << "Filtering app1 pattern: " << pattern << endl;

                // 1. Discard if already processed
                if(isInputProcessed(get<0>(message.getTuple()))) {
                    ackInputProcessed(message);
                } else {
                    // 2. Process the input
                    string line = get<2>(message.getTuple());
                    vector<Tuple> processedInputs;
                    if (line.find(pattern) != string::npos) {
                        Tuple outputTuple = make_tuple(get<0>(message.getTuple()), get<1>(message.getTuple()), line);
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

void FilterTask::printOutput(Tuple outputTuple) {
    cout << "Filter Output: " << get<0>(outputTuple) << " - " << get<1>(outputTuple) << " - " << get<2>(outputTuple) << endl;
}