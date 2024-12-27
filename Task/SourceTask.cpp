#include "SourceTask.h"
#include <fstream>
#include <thread>
#include <chrono>

SourceTask::SourceTask(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) : Task(scheduleMessage, manager, dfs) {
    cout << "Source task created" << endl;
    hydfsSrcFile = scheduleMessage.getSrcFileName();
    string localFileName = scheduleMessage.getTaskId() + "_" + hydfsSrcFile;
    dfs->requestFile(hydfsSrcFile, localFileName);
    srcFilePath = dfs->getLocalFilesPath(localFileName);
}

SourceTask::~SourceTask() {
    cout << "Source task destroyed" << endl;
}

void SourceTask::startTask() {
    cout<<"Starting source task"<<endl;
    taskRunning = true;
    thread messageProcessorThread(&SourceTask::messageProcessorThread, this);
    taskThreads.emplace_back(std::move(messageProcessorThread));
    thread mainThread(&SourceTask::mainTaskThread, this);
    taskThreads.emplace_back(std::move(mainThread));
    taskThreads.emplace_back(&Task::retryOutputsThread, this);
}

void SourceTask::mainTaskThread() {


    cout << "Source task started" << endl;
    int startOffset = scheduleMessage.getStartOffset();
    int endOffset = scheduleMessage.getEndOffset();
    int startLineNumber = scheduleMessage.getStartLineNumber();
    ifstream file(srcFilePath);
    // start reading from startOffset to endOffset and create a tuple of (message.src_fileName:lineNumber, message.src_fileName:lineNumber, lineContent)

    if (!file.is_open()) {
        cerr << "Error opening file: " << srcFilePath << endl;
        return;
    }

    int lineNumber = startLineNumber;
    file.seekg(startOffset, ios::beg);
    string line;
    // cout<<"Reading file from line number "<<lineNumber<<endl;
    while (getline(file, line) && file.tellg() <= endOffset) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        string uniqueId = hydfsSrcFile + ":" + to_string(lineNumber);
        Tuple outputTuple = make_tuple(
            uniqueId,
            uniqueId,
            line
        );
        
        // addTuple(uniqueId, outputTuple);
        sendOutputToNextTask(outputTuple);       

        lineNumber++;
    }
    cout<<"Size of buffer - "<<getOutputBufferSize()<<endl;
    cout << "Source task finished" << endl;
    // cout<<lineNumber<<endl; 
    // taskRunning = false;
}

void SourceTask::messageProcessorThread() {
    RainStormMessage message;
    while(taskRunning) {
        if(getMessageFromQueue(message)) {
            // cout << "Source task received message: " << RainStormMessage::rainStormMessageTypeToString(message.getType()) << endl;
            // cout << "for input: " << get<0>(message.getTuple()) << endl;
            if(message.getType() == RainStormMessageType::INPUT_PROCESSED) {
                removeOutputFromBuffer(get<0>(message.getTuple()));
            } else {
                cout<<"NOT Expected this message here - source"<<endl;
            }
        }
    }
}