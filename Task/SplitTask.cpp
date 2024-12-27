#include "SplitTask.h"
#include <sstream>

SplitTask::SplitTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) : Task(scheduleMessage, manager, dfs) {
    cout << "Split task created" << endl;
}

SplitTask::~SplitTask() {
    cout << "Split task destroyed" << endl;
}

void SplitTask::mainTaskThread() {
    RainStormMessage message;
    while(taskRunning) {
        if(getMessageFromQueue(message)) {
            // cout << "Split task received message: " << get<0>(message.getTuple()) << endl;
            if(message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                removeOutputFromBuffer(get<0>(message.getTuple()));
            } else if (message.getType() == RainStormMessageType::INPUT) {

                // 1. Discard if alread processed
                if(isInputProcessed(get<0>(message.getTuple()))) {
                    ackInputProcessed(message);
                } else {
                    
                    // 2. Process the input
                    vector<string> words = splitLine(get<2>(message.getTuple()));
                    string uniqueIdPrefix = get<0>(message.getTuple());

                    // 3. log the inputs, outputs and state


                    // 4. Ack previous task
                    markInputProcessed(get<0>(message.getTuple()));
                    ackInputProcessed(message);


                    // 5. Send output to next task and buffer
                    for (int i = 0; i < words.size(); i++) {
                        string uniqueId = uniqueIdPrefix + "_" + to_string(i+1);
                        Tuple outputTuple = make_tuple(uniqueId, words[i], "1");
                        sendOutputToNextTask(outputTuple);
                    }

                }

            } else {
                cout<<"NOT Expected this message here - split"<<endl;
            }

        }
    }
}

vector<string> SplitTask::splitLine(string line) {
    vector<string> words;
    stringstream ss(line);
    string word;
    while(ss >> word) {
        words.push_back(word);
    }
    return words;
}