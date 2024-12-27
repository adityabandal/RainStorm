#include "ResourceManager.h"
#include "utils.h"
#include "RainStormMessage.h" 
#include <unistd.h>
#include <climits>
#include <sstream>

#define SOURCE_TASK "source"


ResourceManager::ResourceManager(char* port, Node selfNode, HyDFS* _dfs) : Manager(port, selfNode, _dfs) {
    cout<<"Setting up ResourceManager\n";
    // tasksToBeFailed = {"filter_0", "filter_1"};
}

void ResourceManager::startListening(int sockfd) {
    
    if (listen(sockfd, 10) < 0) {
        std::cerr << "Listen failed: " << strerror(errno) << std::endl;
        return;
    }

    while(running) {
        struct sockaddr_in clientAddr;
        socklen_t clientAddrLen = sizeof(clientAddr);

        int clientSockfd = accept(sockfd, (struct sockaddr *)&clientAddr, &clientAddrLen);
        if(clientSockfd < 0) {
            std::cerr << "Error accepting connection" << std::endl;
            continue;
        }

        thread clientThread(&ResourceManager::messageHandler, this, clientSockfd);
        threads.emplace_back(std::move(clientThread));

        // char buffer[1024] = {0};
        // int bytesRead = 0;

        // if ((bytesRead = recv(clientSockfd, buffer, sizeof(buffer), 0)) > 0) {
        //     buffer[bytesRead] = '\0';
        //     std::string command(buffer);
        //     std::cout << getCurrentFullTimestamp() << " : " << "Received command: " << command << std::endl;
        //     executeCommand(command);
        // }
        
    }
    close(sockfd);
}

void ResourceManager::messageHandler(int sockfd) {
    RainStormMessage message = receiveMessageFromSocket(sockfd);
    if(message.getType() == RainStormMessageType::COMMAND) {
        std::string command = message.getCommand();
        executeCommand(command);
    } else if (message.getType() == RainStormMessageType::OUTPUT) {
        cout<<"Received output from task: "<<endl;
        bool isStateFull = message.getIsStateFull();
        // string fileName = message.getFileName();
        if (isStateFull) {
            writeToFileStatefull(message);
        } else {
            writeToFile(message);
        }
    }
}

void ResourceManager::writeToFileStatefull(RainStormMessage message) {
    string fileName = message.getDestFileName();
    unordered_map<string, int> stateMap = message.getState();
    for (auto& p : stateMap) {
        cout << p.first << " : " << p.second << endl;
    }

    string tempOutput = fileName + "_temp_output_" + getCurrentTSinEpoch();
    // dfs->createEmptyFile(tempOutput);
    string path = dfs->getLocalFilesPath(tempOutput);

    ofstream file(path, ios::out | ios::trunc);

    for (auto& p : stateMap) {
        file << p.first << " : " << p.second << endl;
    }
    file.close();

    cout<<"Writing output to file stateful: "<<fileName  << " "<< stateMap.size()<<endl;
    dfs->appendFile(tempOutput, fileName);
    remove(path.c_str());
}

void ResourceManager::writeToFile(RainStormMessage message) {
    string fileName = message.getDestFileName();
    vector<Tuple> toBeOutput;
    vector<Tuple> batchedOutputs = message.getBatchedOutputs();
    for (auto& p : batchedOutputs) {
        if (processed_outputs.find(get<0>(p)) != processed_outputs.end()) {
            continue;
        }
        toBeOutput.push_back(p);
        processed_outputs.insert(get<0>(p));
        cout << get<0>(p) << " : " << get<1>(p) << " : " << get<2>(p) << endl;
    }
    string tempOutput = fileName + "_temp1_output_" + getCurrentTSinEpoch();
    // dfs->createEmptyFile(tempOutput);
    string path = dfs->getLocalFilesPath(tempOutput);

    ofstream file(path, ios::out | ios::trunc);

    for (auto& p : toBeOutput) {
        file << "o/p: " << get<0>(p) << " : " << get<1>(p) << " : " << get<2>(p) << endl;
    }
    file.close();

    cout<<"Writing output to file: "<<fileName << ", outputs = "<<toBeOutput.size()<<" total = "<<processed_outputs.size()<<endl;
    dfs->appendFile(tempOutput, fileName);

    remove(path.c_str());

}


ResourceManager::~ResourceManager() {
    cout<<"Destructor called for RM\n";
    cout<<selfNode.getNodeId()<<endl;

}

void ResourceManager::executeCommand(string command) {
    std::cout << getCurrentFullTimestamp() << " : " << "Received command: " << command << std::endl;
    string cmd, op1, op2, src_hydfs, dest_hydfs, param1 = "", numTaskStr;
    int numTasks = 0;
    stringstream ss(command);
    std::getline(ss, cmd, ' ');
    std::getline(ss, op1, ' ');
    std::getline(ss, op2, ' ');
    std::getline(ss, src_hydfs, ' ');
    std::getline(ss, dest_hydfs, ' ');
    ss >> numTasks;
    ss.ignore();
    std::getline(ss, param1);



    // size_t pos = op1.find('-');
    // std::string param1 = "";
    // if (pos != std::string::npos) {
    //     param1 = op1.substr(pos + 1);
    //     op1 = op1.substr(0, pos);
    // }

    cout<<"Command: "<<cmd<<endl;
    cout<<"Op1: "<<op1<<endl;
    cout<<"Op2: "<<op2<<endl;
    cout<<"Src: "<<src_hydfs<<endl;
    cout<<"Dest: "<<dest_hydfs<<endl;
    cout<<"NumTasks: "<<numTasks<<endl;
    cout<<"Param1: "<<param1<<endl;

    // validate command
    if (cmd != "rainstorm") {
        std::cerr << "Invalid command: " << cmd << std::endl;
        return;
    }
    if (numTasks <= 0) {
        std::cerr << "Invalid number of tasks: " << numTasks << std::endl;
        return;
    }

    // dfs->createFile(src_hydfs, src_hydfs);

    dfs->createEmptyFile(dest_hydfs);

    scheduleTask(op2, true, numTasks, src_hydfs, dest_hydfs);
    state.addTaskChain(op1, op2);
    scheduleTask(op1, false, numTasks, src_hydfs, dest_hydfs, state.getRoutingTableOfTask(op2), param1);
    state.addTaskChain(SOURCE_TASK, op1);
    scheduleTask(SOURCE_TASK, false, numTasks, src_hydfs, dest_hydfs, state.getRoutingTableOfTask(op1));
}

void ResourceManager::scheduleTask(string op, bool isLastTask, int numTasks, string src_hydfs, string dest_hydfs, unordered_map<int, pair<string, string>> prevRoutingTable, string param1) { 
    vector<vector<int>> offsets;
    RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::SCHEDULE);
    if(op == SOURCE_TASK) {
        string localFileName = "rainstormTestFile";
        dfs->requestFile(src_hydfs, localFileName);
        string srcFilePath = dfs->getLocalFilesPath(localFileName);
        offsets = divideFile(srcFilePath, numTasks);
    }
    message.setTaskName(op);
    message.setIsEndTask(isLastTask);
    message.setSrcFileName(src_hydfs);
    message.setDestFileName(dest_hydfs);
    message.setTaskType(RainStormMessage::getTaskTypeFromName(op));
    message.setParam1(param1);
    message.setLeaderNodeId(selfNode.getNodeId());
    if(prevRoutingTable.size() > 0) {
        message.setRoutingTable(prevRoutingTable);
    }
    state.setScheduleMessageForTask(op, message);
    for(int i=0;i<numTasks;i++){
        string taskId = op + "_" + to_string(i) + "_" + getCurrentTSinEpoch();
        message.setTaskId(taskId);
        if(op == SOURCE_TASK) {
            message.setStartOffset(offsets[i][0]);
            message.setEndOffset(offsets[i][1]);
            message.setStartLineNumber(offsets[i][2]);
        }
        scheduleTask(message);
    }
}

string getTaskIdPrefix(string taskId) {
    // taskid is task_i_timestamp, remove _timestamp
    size_t pos = taskId.find_last_of('_');
    return taskId.substr(0, pos);
}

void ResourceManager::scheduleTask(RainStormMessage message) {
    if(tasksToBeFailed.find(getTaskIdPrefix(message.getTaskId())) != tasksToBeFailed.end()) {
        cout<<"Task to be failed: "<<message.getTaskId()<<endl;
        message.setIsToBeFailed(true);
        tasksToBeFailed.erase(message.getTaskId());
    }
    string nodeId = getNodeWithLeastJobs();

    if (nodeId == "") {
        std::cerr << "No nodes available to schedule task" << std::endl;
        return;
    }

    cout<<"Scheduling task "<<message.getTaskId()<<" on node "<<nodeId<<endl;

    int tmpSock = getStormSocketForNode(nodeIdToNode[nodeId]);
    if (tmpSock == -1) {
        std::cerr << "Error getting socket for node: " << nodeId << std::endl;
        return;
    }
    sendMessageToSocket(tmpSock, message);
    RainStormMessage response = receiveMessageFromSocket(tmpSock);

    if(response.getType() == RainStormMessageType::STARTED) {
        state.addTaskId(message.getTaskId(), nodeId);
    } else {
        std::cerr << "Error starting task on node: " << nodeId << std::endl;
    }

    close(tmpSock);
}
    

void ResourceManager::membershipUpdate(vector<Node> nodes) {
    cout << getCurrentFullTimestamp()<<" : "<<"Membership update\n";
    std::unique_lock<std::mutex> lock(nodeMapMutex);
    for (Node node : nodes) {
        if(node.getStatus() == Status::alive) {
            nodeIdToNode[node.getNodeId()] = node;
        } else if (node.getStatus() == Status::failed || node.getStatus() == Status::left) {
            nodeIdToNode.erase(node.getNodeId());
            thread t(&ResourceManager::handleFailedNode, this, node.getNodeId());
            threads.push_back(move(t));
        }
    }
    cout << getCurrentFullTimestamp()<<" : "<<"Membership update done\n";
}

void ResourceManager::handleFailedNode(string nodeId) {
    cout << getCurrentFullTimestamp()<<" : "<<"Handling failed node: "<<nodeId<<endl;
    vector<string> taskIds = state.getTasksOnNode(nodeId);
    for(string taskId : taskIds) {
        cout<<"Re-scheduling task: "<<taskId<<endl;
        string taskName = state.getTaskNameFromTaskId(taskId);
        RainStormMessage message = state.getScheduleMessageForTask(taskName);
        message.setTaskId(taskId);
        scheduleTask(message);

        string prevTask = state.getPrevTask(taskName);

        if(prevTask != "") {
            cout<<"Updating routing table for task: "<<prevTask<<endl;
            RainStormMessage prevMessage = state.getScheduleMessageForTask(prevTask);
            unordered_map<int, pair<string, string>> routingTable = state.getRoutingTableOfTask(taskName);
            prevMessage.setRoutingTable(routingTable);
            state.setScheduleMessageForTask(prevTask, prevMessage);
            updateRoutingTableForTask(prevTask);
        }
    }
    state.deleteNode(nodeId);
}

void ResourceManager::updateRoutingTableForTask(string taskName) {
    RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::UPDATE_ROUTING_TABLE);
    message.setRoutingTable(state.getScheduleMessageForTask(taskName).getRoutingTable());
    vector<pair<string, string>> taskIds = state.getTaskIdNodeIdPairForTask(taskName);
    cout<<"Updating routing table for task: "<<taskName<<" size - "<<taskIds.size()<<endl;
    for(pair<string, string> taskIdNodeId : taskIds) {
        cout<<"Updating routing table for task: "<<taskIdNodeId.first<<endl;
        message.setTaskId(taskIdNodeId.first);
        if(nodeIdToNode.find(taskIdNodeId.second) == nodeIdToNode.end()) {
            std::cerr << "Node not found for task to update routing table: " << taskIdNodeId.first << std::endl;
            continue;
        }
        cout<<"Updating routing table for task: "<<taskIdNodeId.first<<" on node: "<<taskIdNodeId.second<<endl;
        int tmpSock = getStormSocketForNode(nodeIdToNode[taskIdNodeId.second]);
        if (tmpSock == -1) {
            std::cerr << "Error getting socket for node: " << taskIdNodeId.second << std::endl;
            return;
        }
        sendMessageToSocket(tmpSock, message);
        close(tmpSock);
    }
}


string ResourceManager::getNodeWithLeastJobs() {
    int minJobs = INT_MAX;
    string minNodeId = "";
    for(auto &pair : nodeIdToNode) {
        if (state.getNumberOfTasksOnNode(pair.first) < minJobs) {
            minJobs = state.getNumberOfTasksOnNode(pair.first);
            minNodeId = pair.first;
        }
    }
    return minNodeId;
}

bool ResourceManager::runCommandLine(string command) {
    if (command == "rn") {
        cout << getCurrentFullTimestamp()<<" : \n"<<nodeIdToNode.size();
    } else if (command == "rjobs") {
        cout << getCurrentFullTimestamp()<<" : \n";
        for(auto p: nodeIdToNode) {
            cout << p.first << " : " << state.getNumberOfTasksOnNode(p.first) << endl;
        }
    } else if(command.find("rainstorm") == 0){
        executeCommand(command);
    } else if (command == "state") {
        cout << getCurrentFullTimestamp()<<" : \n";
        cout << state;
    } else if (command.find("fail") == 0) {
        string taskId = command.substr(5);
        tasksToBeFailed.insert(taskId);
    } else if (command == "cnt") {
        cout << getCurrentFullTimestamp()<<" : \n";
        cout << "Processed outputs: " << processed_outputs.size() << endl;
    }
    else {
        cout<<"RM: Command not found\n";
        return false;
    }
    return true;
}
