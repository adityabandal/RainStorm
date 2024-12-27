#include "NodeManager.h"
#include "utils.h"
#include "Operations.h"
#include "Task/SourceTask.h"
#include "Task/SplitTask.h"
#include "Task/CountTask.h"
#include "Task/FilterTask.h"
#include "Task/SelectTask.h"
#include "Task/CountCategoryTask.h"
#include "Task/FilterSignPostTask.h"
#include <unistd.h>

NodeManager::NodeManager(char* port, Node _selfNode, HyDFS* _dfs, Node _leaderNode) : Manager(port, _selfNode, _dfs) {
    leaderNode = _leaderNode;
    std::unique_lock<std::mutex> lock(nodeMapMutex);
    nodeIdToNode[_selfNode.getNodeId()] = _selfNode;
    totalMsgs = 0;
    cout<<"Self Node: "<<_selfNode.getNodeId()<<endl;
    cout<<"Setting up NodeManager\n";
}

void NodeManager::startListening(int socket) {
    if (listen(socket, 1024) < 0) {
        perror("Listen failed");
        close(socket);
        exit(EXIT_FAILURE);
    }
    while (running) {
        struct sockaddr_in clientAddr;
        socklen_t clientAddrLen = sizeof(clientAddr);
        int clientSocket = accept(socket, (struct sockaddr *)&clientAddr, &clientAddrLen);
        if (clientSocket < 0) {
            std::cerr << "Error accepting connection: " << strerror(errno) << std::endl;
            continue;
        }

        thread clientThread(&NodeManager::messageHandler, this, clientSocket);
        threads.emplace_back(std::move(clientThread));
    }
    for(auto& p: taskIdToTask){
        p.second->joinTaskThreads();
    }
}

void NodeManager::messageHandler(int sockfd) {
    RainStormMessage message = receiveMessageFromSocket(sockfd);

    if (message.getType() == RainStormMessageType::SCHEDULE) {
        // cout << getCurrentFullTimestamp() << " : " << "Received command: " << message << std::endl;


        thread taskThread(&NodeManager::startTask, this, message);
        threads.emplace_back(std::move(taskThread));

        RainStormMessage response = RainStormMessage::getRainStormMessage(RainStormMessageType::STARTED);
        response.setTaskId(message.getTaskId());
        sendMessageToSocket(sockfd, response);

    } else if (message.getType() == RainStormMessageType::UPDATE_ROUTING_TABLE) {
        if(taskIdToTask.find(message.getTaskId()) == taskIdToTask.end()){
            cerr<<"Task not found for updating routing table\n";
        } else {
            taskIdToTask[message.getTaskId()]->updateRoutingTable(message.getRoutingTable());
        }
    } else if (message.getType() == RainStormMessageType::INPUT) {
        // cout<<"Received input message for task: "<<message.getTaskId()<<endl;
        // cout<<get<0>(message.getTuple())<<" - "<<get<1>(message.getTuple()) << " - "<< get<2>(message.getTuple())<<endl;
        // cout<<++totalMsgs<<endl;
        if(taskIdToTask.find(message.getTaskId()) == taskIdToTask.end()){
            cerr<<"Task not found for input message\n";
        } else {
            // cout<<"Adding message to queue for task: "<<message.getTaskId()<<endl;
            taskIdToTask[message.getTaskId()]->addMessageToQueue(message);
        }
    } else if (message.getType() == RainStormMessageType::INPUT_PROCESSED) {
        // cout<<"Received ack message for task: "<<message.getTaskId()<<endl;
        // cout<<get<0>(message.getTuple())<<" - "<<get<1>(message.getTuple()) << " - "<< get<2>(message.getTuple())<<endl;
        if(taskIdToTask.find(message.getTaskId()) == taskIdToTask.end()){
            cerr<<"Task not found for ack message\n";
        } else {
            taskIdToTask[message.getTaskId()]->addMessageToQueue(message);
        }
    }
    else {
        std::cerr << "Invalid message type received" << std::endl;
    }
    close(sockfd);
}

void NodeManager::startTask(RainStormMessage message) {
    string taskName = message.getTaskName();
    Task* task;
    if (taskName == "source") {
        // Handle source task
        cout << "Starting source task" << endl;
        task = new SourceTask(message, this, dfs);
    } else if (taskName == "split") {
        // Handle split task
        cout << "Starting split task" << endl;
        task = new SplitTask(message, this, dfs);
    } else if (taskName == "count") {
        // Handle count task
        cout << "Starting count task" << endl;
        task = new CountTask(message, this, dfs);
    } else if (taskName == "filter") {
        // Handle filter task
        cout << "Starting filter task" << endl;
        task = new FilterTask(message, this, dfs, message.getParam1());
    } else if (taskName == "select") {
        // Handle select task
        cout << "Starting select task" << endl;
        task = new SelectTask(message, this, dfs);
    } else if (taskName == "filterSignPost") {
        // Handle filterSignPost task
        cout << "Starting filterSignPost task" << endl;
        task = new FilterSignPostTask(message, this, dfs, message.getParam1());
    }
    else if (taskName == "countCategory") {
        // Handle countCategory task
        cout << "Starting countCategory task" << endl;
        task = new CountCategoryTask(message, this, dfs);
    }
    else {
        cout << "Unknown task: " << taskName << endl;
        return;
    }

    taskIdToTask[message.getTaskId()] = unique_ptr<Task>(task);
    task->startTask();
}



void NodeManager::executeCommand(std::string command) {
    std::cout << getCurrentFullTimestamp() << " : " << "NM:Received command: " << command << std::endl;
    cout<<"Leader Node: "<<leaderNode.getNodeId()<<endl;
    int sockToLeader = getStormSocketForNode(leaderNode);
    if(sockToLeader == -1){
        cerr<<"Error getting socket for leader"<<endl;
        return;
    }
    cout<<"sending command to leader "<<sockToLeader<<"\n";
    // send command to sockToLeader
    RainStormMessage message = RainStormMessage::getRainStormMessage(RainStormMessageType::COMMAND);
    message.setCommand(command);
    sendMessageToSocket(sockToLeader, message);
    // if ((send(sockToLeader, command.c_str(), command.size(), 0)) < 0) {
    //     std::cerr << "Error sending message: " << strerror(errno) << std::endl;
    // }

    close(sockToLeader);
}

NodeManager::~NodeManager() {
    cout<<"Destructor called for NodeManager\n";
    cout<<leaderNode.getNodeId()<<endl;
}


bool NodeManager::runCommandLine(string command) {
    cout<<"NM: Command: "<<command<<endl;

    if(command.find("rainstorm") == 0){
        executeCommand(command);
    } else if(command == "rt"){
        for(auto& p: taskIdToTask){
            cout<<p.first<<" : "<<endl;
            unordered_map<int, pair<string, string>> rt = p.second->getRoutingTable();
            for(auto& q: rt){
                cout<<q.first<<" : "<<q.second.first<<" - "<<q.second.second<<endl;
            }
        }
    } 
    
    else {
        cout<<"NM: Command not found\n";
        return false;
    }
    return true;
}

void NodeManager::membershipUpdate(vector<Node> nodes) {
    cout << getCurrentFullTimestamp()<<" : "<<"Membership update\n";
    std::lock_guard<std::mutex> lock(nodeMapMutex);
    for (Node node : nodes) {
        if(node.getStatus() == Status::alive) {
            nodeIdToNode[node.getNodeId()] = node;
        } else if (node.getStatus() == Status::failed || node.getStatus() == Status::left) {
            nodeIdToNode.erase(node.getNodeId());
        }
    }
    cout << getCurrentFullTimestamp()<<" : "<<"Membership update done\n";
}

