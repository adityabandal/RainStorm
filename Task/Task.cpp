#include "Task.h"
#include <unistd.h>
#include <sstream>
#include <fstream>

#define RETRY_INTERVAL 5
#define RETRY_TIMEOUT_MS 4000
#define JOURNAL_FLUSH_INTERVAL_MS 3000
#define RESULT_FILE_WRITER_MS 3000

Task::Task(RainStormMessage scheduleMessage, Manager* manager, HyDFS* dfs) 
    : scheduleMessage(scheduleMessage), manager(manager), dfs(dfs) {
        routingTable = scheduleMessage.getRoutingTable();
        taskJournal = scheduleMessage.getTaskId() + "_task_journal";
        taskType = scheduleMessage.getTaskType();
        isStateChanged = false;

        numberOfRecordsOutputed = 0;

        if(scheduleMessage.getTaskType() != TaskType::SOURCE) { 
            if(!dfs->isFilePresentInHyDFS(taskJournal)) {
                dfs->createEmptyFile(taskJournal);
            } else {
                cout<<"Replaying journal for task: "<<scheduleMessage.getTaskId()<<endl;
                replayJournal();
            }
        }
    }

Task::~Task() {
    cout << "Task destroying" << endl;
}

void Task::startTask() {
    cout << "Task starting" << endl;
    taskRunning = true;
    taskThreads.emplace_back(&Task::mainTaskThread, this);
    if(!scheduleMessage.getIsEndTask()) {
        taskThreads.emplace_back(&Task::retryOutputsThread, this);
    } 
    else {
        // thread t(&Task::outputsWriterThread, this);
        // taskThreads.emplace_back(move(t));
    }
    if(scheduleMessage.getIsToBeFailed()) {
        taskThreads.emplace_back(&Task::killerThread, this);
    }

    if(scheduleMessage.getTaskType() != TaskType::SOURCE) {
        thread t(&Task::journalWriterThread, this); 
        taskThreads.emplace_back(move(t));
    }
}

void Task::outputsWriterThread() {
    while(taskRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(RESULT_FILE_WRITER_MS));
        if(taskType == TaskType::AGGREGATE) {
            // writeStateToDestinationFile();
        } else {
            // writeOutputToDestinationFile();
        }
    }
}

void Task::writeStateToDestinationFile() {
    
    // lock_guard<mutex> lock(state_mutex);
    if(!isStateChanged) {
        return;
    }
    string resultFile = scheduleMessage.getDestFileName();
    string tempOutput = scheduleMessage.getTaskId() + "_temp_output";
    string path = dfs->getLocalFilesPath(tempOutput);

    ofstream file(path, ios::out | ios::trunc);

    for (auto& p : stateMap) {
        file << p.first << " : " << p.second << endl;
    }

    file.close();

    cout<<"Writing output to file: "<<resultFile<<endl;
    dfs->appendFile(tempOutput, resultFile);

    remove(path.c_str());
    isStateChanged = false;
    // state_mutex.unlock();

}

void Task::writeOutputToDestinationFile() {

    string resultFile = scheduleMessage.getDestFileName();
    // lock_guard<mutex> lock(output_mutex);
    unordered_map<string, Tuple> cpy = uniqueIdToTuple;
    if(cpy.empty()) {
        return;
    }
    string tempOutput = scheduleMessage.getTaskId() + "_temp_output";
    string path = dfs->getLocalFilesPath(tempOutput);

    ofstream file(path, ios::out | ios::trunc);

    for (auto& p : cpy) {
        file << get<0>(p.second) << " : " << get<1>(p.second) << " : " << get<2>(p.second) << endl;
    }

    file.close();
    cout<<"Writing output to file: "<<resultFile<<endl;
    dfs->appendFile(tempOutput, resultFile);
    remove(path.c_str());
    for(auto& p: cpy){
        receivedAcks.insert(get<0>(p.second));
    }

    uniqueIdToTuple.clear();
    uniqueIdToSentTS.clear();

    // output_mutex.unlock();
}

void Task::joinTaskThreads() {
    taskRunning = false;
    for (auto& t : taskThreads) {
       if(t.joinable()) t.join();
    }
}

void Task::addMessageToQueue(RainStormMessage message) {
    lock_guard<mutex> lock(queque_mtx);
    messageQueue.push(message);
    queue_cv.notify_one();
}

bool Task::getMessageFromQueue(RainStormMessage& message) {
    std::unique_lock<std::mutex> lock(queque_mtx);
    queue_cv.wait(lock, [this]() { return !messageQueue.empty() || !taskRunning; });

    if (messageQueue.empty()) {
        return false; // Queue is empty and stop flag is set
    }

    message = messageQueue.front();
    messageQueue.pop();
    
    return true;
}
    

void Task::sendOutputToNextTask(Tuple outputTuple) {
    // tuple is of the form (uniqueId, key, value)
    // always hash the key and NOT the uniqueId
    int hash = hashFunction(get<1>(outputTuple), routingTable.size());
    // cout<<"Hash = "<<hash<<endl;
    std::unique_lock<std::mutex> lock(rt_mtx);
    if (routingTable.find(hash) == routingTable.end()) {
        cerr << "No node found for hash: " << hash << endl;
        return;
    }
    pair<string, string> nodeIdTaskId = routingTable[hash];
    lock.unlock();
    // cout<<nodeIdTaskId.first<<endl;
    // cout<<nodeIdTaskId.second<<endl;

    int sock = manager->getStormSocketForNode(nodeIdTaskId.first);

    if (sock == -1) {
        cerr << "Error getting socket for node" << endl;
        return;
    }

    RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::INPUT);
    message.setTaskId(nodeIdTaskId.second);
    message.setTuple(outputTuple);
    // cout<<"SRC NODE ID: "<<manager->getSelfNode().getNodeId()<<endl;
    message.setSourceNodeId(manager->getSelfNode().getNodeId());
    message.setSourceTaskId(scheduleMessage.getTaskId());
    manager->sendMessageToSocket(sock, message);
    close(sock);
    // cout<<"Message sent to next task\n";

    addOutputToBuffer(outputTuple, getCurrentTSinEpoch());
    // cout<<"Sending output to next task\n";
    // cout<<hash<<" : "<<routingTable[hash].first<<endl;

    // TODO send outputTuple to next task
    // ignore failure, it will be retried later

}

void Task::updateRoutingTable(unordered_map<int, pair<string, string>> newRoutingTable) {
    lock_guard<mutex> lock(rt_mtx);
    routingTable = newRoutingTable;
}

void Task::ackInputProcessed(RainStormMessage message) {
    // cout<<"Sending ack for input processed\n";
    int sock = manager->getStormSocketForNode(message.getSourceNodeId());
    if (sock == -1) {
        cerr << "Error getting socket for node" << endl;
        return;
    }

    RainStormMessage ackMessage = RainStormMessage::getRainStormMessage(RainStormMessageType::INPUT_PROCESSED);
    ackMessage.setTaskId(message.getSourceTaskId());
    ackMessage.setSourceNodeId(message.getSourceNodeId());
    ackMessage.setTuple(message.getTuple());
    manager->sendMessageToSocket(sock, ackMessage);
    close(sock);

}

void Task::addOutputToBuffer(Tuple outputTuple, string sentTS) {
    lock_guard<mutex> lock(output_mutex);
    if(uniqueIdToTuple.find(get<0>(outputTuple)) == uniqueIdToTuple.end()) {
        numberOfRecordsOutputed++;
        uniqueIdToTuple[get<0>(outputTuple)] = outputTuple;
    }
    uniqueIdToSentTS[get<0>(outputTuple)] = sentTS;
}

void Task::removeOutputFromBuffer(string uniqueId) {
    lock_guard<mutex> lock(output_mutex);
    uniqueIdToTuple.erase(uniqueId);
    uniqueIdToSentTS.erase(uniqueId);
}

void Task::retryOutputsThread() {
    while(taskRunning) {
        
        sleep(RETRY_INTERVAL);

        std::unique_lock<std::mutex> lock(output_mutex);
        unordered_map<string, string> uniqueIdToSentTSCopy = uniqueIdToSentTS;
        lock.unlock();
        cout<<"Retrying outputs for "<<scheduleMessage.getTaskId()<<", Size of buffer - "<<uniqueIdToTuple.size()<<endl;
        cout<<"Taks - "<<scheduleMessage.getTaskId()<<", inputs "<<processedInputs.size()<<", outputs "<<numberOfRecordsOutputed<<endl;
        for (auto& entry : uniqueIdToSentTSCopy) {
            string uniqueId = entry.first;
            // cout<<uniqueId<<endl;
            string sentTS = getLastSentTS(uniqueId);
            if(sentTS == "0") {
                continue;
            }
            int diff = differenceWithCurrentEpoch(sentTS);
            // cout<<"Difference: "<<diff<<endl;
            if (diff > RETRY_TIMEOUT_MS) {
                sendOutputToNextTask(getOutputTuple(uniqueId));
            } 
            // else if (diff == 0) {
            //     cout<<"Something weird with this output, retrying\n";
            //     sendOutputToNextTask(getOutputTuple(uniqueId));
            // }
        }
    }
}

std::vector<std::string> Task::extractColumns(const std::string& line) {
    std::vector<std::string> columns;
    std::stringstream ss(line);
    std::string item;

    // TODO: verify if this works for data conatining commas and quotes

    bool inQuotes = false;
    while (ss.good()) {
        char c = ss.get();
        if (c == '"') {
            inQuotes = !inQuotes;
        } else if (c == ',' && !inQuotes) {
            columns.push_back(item);
            item.clear();
        } else {
            item += c;
        }
    }
    columns.push_back(item); // add the last column

    return columns;
}

void Task::replayJournal(){
    string localJournal = taskJournal + "_local";
    dfs->requestFile(taskJournal, localJournal);

    string localJournalPath = dfs->getLocalFilesPath(localJournal);

    ifstream file(localJournalPath);
    if (!file.is_open()) {
        cerr << "Error opening file: " << localJournalPath << endl;
        return;
    }

    string line;
    while (getline(file, line)) {
        stringstream ss(line);
        string word;
        ss >> word;
        if (word == "INPUT") {
            string uniqueId;
            int outputs;
            ss >> uniqueId >> outputs;
            processedInputs.insert(uniqueId);
            vector<Tuple> outputTuples;
            for (int i = 0; i < outputs; i++) {
                string serTuple, uniqueId, key, value;
                getline(file, serTuple);
                stringstream tupleStream(serTuple);
                getline(tupleStream, uniqueId, ',');
                getline(tupleStream, key, ',');
                getline(tupleStream, value);
                // cout<<uniqueId<<"##"<<key<<"##"<<value<<endl;
                if(taskType == TaskType::AGGREGATE) {
                    int cnt = stoi(value);
                    stateMap[key] = cnt;
                } else {
                    Tuple outputTuple = make_tuple(uniqueId, key, value);
                    uniqueIdToTuple[uniqueId] = outputTuple;
                    uniqueIdToSentTS[uniqueId] = getCurrentTSinEpoch();
                    numberOfRecordsOutputed++;
                }
            }
            markInputProcessed(uniqueId);
        } else if (word == "ACK") {
            string uniqueId;
            ss >> uniqueId;
            uniqueIdToTuple.erase(uniqueId);
            uniqueIdToSentTS.erase(uniqueId);
        } else if (word == "FILE_START") {
            // do nothing
        } else {
            cerr << "Unknown journal entry: " << word << endl;
        }
    }

    if(taskType == TaskType::AGGREGATE) {
        for(auto& p : stateMap) {
            numberOfRecordsOutputed+=p.second;
        }
    }
}


void Task::killerThread() {
    taskRunning = false;
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    exit(1);
}

void Task::addToJournalBuffer(string uniqueId, vector<Tuple> outputs, RainStormMessage message) {
    lock_guard<mutex> lock(journal_mutex);
    journalBuffer[uniqueId] = outputs;
    inputMessage[uniqueId] = message;
}

void Task::addAckToJournalBuffer(string uniqueId) {
    lock_guard<mutex> lock(journal_mutex);
    receivedAcks.insert(uniqueId);
}

void Task::journalWriterThread() {
    while(taskRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(JOURNAL_FLUSH_INTERVAL_MS));
        if(getJournalBufferSize() > 0) {
            if(taskType == TaskType::AGGREGATE) {
                appendBufferToJournalForStateful();
            } else {
                appendBufferToJournal();
            }
        }
    }
}

void Task::appendBufferToJournal() {
    cout<<"Taks - "<<scheduleMessage.getTaskId()<<", inputs "<<processedInputs.size()<<", outputs "<<numberOfRecordsOutputed<<endl;
    
    ofstream file;
    string localJournal = taskJournal + "_local";
    string localJournalPath = dfs->getLocalFilesPath(localJournal);
    file.open(localJournalPath, ios::app);
    if (!file.is_open()) {
        cerr << "Error opening file: " << localJournalPath << endl;
        return;
    }

    lock_guard<mutex> lock(journal_mutex);
    lock_guard<mutex> lock2(input_mutex);
    lock_guard<mutex> lock3(output_mutex);

    if(scheduleMessage.getIsEndTask()) {
        int sock = manager->getStormSocketForNode(scheduleMessage.getLeaderNodeId());
        if (sock == -1) {
            cerr << "Error getting socket for node" << endl;
            return;
        }
        RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::OUTPUT);
        message.setDestFileName(scheduleMessage.getDestFileName());
        message.setIsEndTask(false);
        unordered_map<string, Tuple> cpy = uniqueIdToTuple;
        vector<Tuple> outputsToBeSent;
        for (auto& p : cpy) {
            outputsToBeSent.push_back(p.second);
        }
        message.setBatchedOutputs(outputsToBeSent);

        uniqueIdToTuple.clear();
        uniqueIdToSentTS.clear();

        manager->sendMessageToSocket(sock, message);
        close(sock);

        // writeOutputToDestinationFile();
    }

    for (auto& entry : journalBuffer) {
        string uniqueId = entry.first;
        if(processedInputs.find(uniqueId) != processedInputs.end()) {
            continue;
        }
        file << "INPUT " << uniqueId << " " << entry.second.size() << endl;
        for (Tuple& outputTuple : entry.second) {
            file << get<0>(outputTuple) << "," << get<1>(outputTuple) << "," << get<2>(outputTuple) << endl;
        }
        processedInputs.insert(uniqueId);
    }

    

    for(string uniqueId : receivedAcks) {
        if(uniqueIdToTuple.find(uniqueId) == uniqueIdToTuple.end()) {
            continue;
        }
        file << "ACK " << uniqueId << endl;
        uniqueIdToTuple.erase(uniqueId);
        uniqueIdToSentTS.erase(uniqueId);
    }

    file.close();

    dfs->appendFile(localJournal, taskJournal);

    remove(localJournalPath.c_str());

    
    for(auto& entry : journalBuffer) {
        ackInputProcessed(inputMessage[entry.first]);
    }

    journalBuffer.clear();
    inputMessage.clear();
    receivedAcks.clear();

}


void Task::appendBufferToJournalForStateful() {
    cout<<"Taks - "<<scheduleMessage.getTaskId()<<", inputs "<<processedInputs.size()<<", outputs "<<numberOfRecordsOutputed<<endl;
    
    ofstream file;
    string localJournal = taskJournal + "_local";
    string localJournalPath = dfs->getLocalFilesPath(localJournal);
    file.open(localJournalPath, ios::app);
    if (!file.is_open()) {
        cerr << "Error opening file: " << localJournalPath << endl;
        return;
    }

    lock_guard<mutex> lock(journal_mutex);
    lock_guard<mutex> lock2(state_mutex);
    lock_guard<mutex> lock3(input_mutex);

    for (auto& entry : journalBuffer) {
        string uniqueId = entry.first;
        if(processedInputs.find(uniqueId) != processedInputs.end()) {
            continue;
        }
        file << "INPUT " << uniqueId << " " << entry.second.size() << endl;
        for (Tuple& outputTuple : entry.second) {
            stateMap[get<1>(outputTuple)] ++;
            numberOfRecordsOutputed++;
            isStateChanged = true;
            file << get<0>(outputTuple) << "," << get<1>(outputTuple) << "," << to_string(stateMap[get<1>(outputTuple)]) << endl;
        }
        processedInputs.insert(uniqueId);
    }

    file.close();

    dfs->appendFile(localJournal, taskJournal);

    remove(localJournalPath.c_str());

    // if(scheduleMessage.getIsEndTask()) {
    //     writeStateToDestinationFile();
    // }

    if(scheduleMessage.getIsEndTask()) {

        int sock = manager->getStormSocketForNode(scheduleMessage.getLeaderNodeId());

        if (sock == -1) {
            cerr << "Error getting socket for node" << endl;
            return;
        }
        RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::OUTPUT);
        message.setDestFileName(scheduleMessage.getDestFileName());
        message.setIsStateFull(true);
        message.setState(stateMap);
        manager->sendMessageToSocket(sock, message);
        close(sock);
    }

    for(auto& entry : journalBuffer) {
        ackInputProcessed(inputMessage[entry.first]);
    }

    journalBuffer.clear();
    inputMessage.clear();
    receivedAcks.clear();
}







