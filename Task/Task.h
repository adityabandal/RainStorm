#ifndef TASK_H
#define TASK_H


#include "../RainStormMessage.h"
#include "../Manager.h"
#include "../utils.h"
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <vector>
#include <string>
#include <thread>
#include <unordered_set>
#include <queue>
using namespace std;

#define Tuple tuple<string, string, string>

class Task {
private:
    TaskType taskType;
    unordered_map<int, pair<string, string>> routingTable;
    mutex rt_mtx, queque_mtx;
    queue<RainStormMessage> messageQueue;
    condition_variable queue_cv;

    mutex input_mutex;
    unordered_set<string> processedInputs;

    mutex output_mutex;
    unordered_map<string, Tuple> uniqueIdToTuple;
    unordered_map<string, string> uniqueIdToSentTS;

    mutex state_mutex;
    unordered_map<string, int> stateMap;
    atomic<bool> isStateChanged;

    mutex journal_mutex;
    unordered_map<string, vector<Tuple>> journalBuffer;
    unordered_map<string, RainStormMessage> inputMessage;
    unordered_set<string> receivedAcks;


    
    Manager* manager;
    HyDFS* dfs;
    

    string taskJournal;

protected:
    atomic<bool> taskRunning;
    vector<thread> taskThreads;
    RainStormMessage scheduleMessage;

public:

    Task(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs);
    ~Task();

    virtual void startTask();
    void joinTaskThreads();

    void addMessageToQueue(RainStormMessage message);
    bool getMessageFromQueue(RainStormMessage& message);

    void sendOutputToNextTask(Tuple outputTuple);
    void ackInputProcessed(RainStormMessage message);
    void updateRoutingTable(unordered_map<int, pair<string, string>> newRoutingTable);

    void retryOutputsThread();
    virtual void mainTaskThread() = 0;
    void outputsWriterThread();
    void killerThread();

    void writeOutputToDestinationFile();
    void writeStateToDestinationFile();
    void addOutputToBuffer(Tuple outputTuple, string sentTS);
    void removeOutputFromBuffer(string uniqueId);

    vector<string> extractColumns(const string& line);
    atomic<int> numberOfRecordsOutputed;

    string getLastSentTS(string uniqueId) {
        lock_guard<mutex> lock(output_mutex);
        if(uniqueIdToSentTS.find(uniqueId) == uniqueIdToSentTS.end()) {
            return "0";
        } else {
            return uniqueIdToSentTS[uniqueId];
        }
    }

    Tuple getOutputTuple(string uniqueId) {
        lock_guard<mutex> lock(output_mutex);
        return uniqueIdToTuple[uniqueId];
    }

    int getOutputBufferSize() {
        lock_guard<mutex> lock(output_mutex);
        return uniqueIdToTuple.size();
    }

    bool isInputProcessed(string uniqueId) {
        lock_guard<mutex> lock(input_mutex);
        return processedInputs.find(uniqueId) != processedInputs.end();
    }

    void markInputProcessed(string uniqueId) {
        lock_guard<mutex> lock(input_mutex);
        processedInputs.insert(uniqueId);
    }

    unordered_map<int, pair<string, string>> getRoutingTable() {
        lock_guard<mutex> lock(rt_mtx);
        return routingTable;
    }

    int getJournalBufferSize() {
        lock_guard<mutex> lock(journal_mutex);
        return journalBuffer.size() + receivedAcks.size();
    }

    


    virtual void replayJournal();

    // journaling
    void addToJournalBuffer(string uniqueId, vector<Tuple> outputs, RainStormMessage message);
    void addAckToJournalBuffer(string uniqueId);
    void journalWriterThread();
    void appendBufferToJournal();
    void appendBufferToJournalForStateful();

    

};

#endif // TASK_H