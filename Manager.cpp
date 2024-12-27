#include "Manager.h"
#include "utils.h"
#include <iostream>
#include <unistd.h>

Manager::Manager(char* port, Node selfNode, HyDFS* dfs) {
    streamPort = port;
    this->dfs = dfs;
    this->selfNode = selfNode;

    streamSocket = setupTCPSocket(streamPort);
    if (streamSocket < 0) {
        std::cerr << "Error setting up socket" << std::endl;
    }
    running = true;
    
}

void Manager::setSelfNode(Node node) {
    selfNode = node;
}

Manager::~Manager() {
    std::cout << "Destructor called for Manager\n";
    std::cout << selfNode.getNodeId() << std::endl;
}

void Manager::start() {
    std::thread listenerThread(&Manager::startListening, this, streamSocket);
    threads.emplace_back(std::move(listenerThread));
}

void Manager::stop() {
    running = false;
    for (std::thread &t : threads) {
        t.join();
    }
}

int Manager::getStormSocketForNode(Node node){
    struct sockaddr_in nodeAddr = getStormAddrFromNode(node);
    if (nodeAddr.sin_addr.s_addr == INADDR_ANY) {
        std::cerr << "Node address is all zeros" << std::endl;
        return -1;
    }
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        std::cerr << "Error creating socket: " << strerror(errno) << std::endl;
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&nodeAddr, sizeof(nodeAddr)) < 0) {
        std::cerr << "Error connecting to node for rainstorm: " << strerror(errno) << std::endl;
        close(sockfd);
        return -1;
    }
    return sockfd;
}

void Manager::sendMessageToSocket(int sockfd, RainStormMessage message){
    std::string serializedMessage = message.serializeRainStormMessage();
    // cout<<"----------------\n"<<serializedMessage<<"\n----------------\n";
    // cout<<"SENDING_MSG - "<<message.getTypeString()<<"\n";
    int bytes = 0;
    if ((bytes = send(sockfd, serializedMessage.c_str(), serializedMessage.size(), 0)) < 0) {
        std::cerr << "Error sending message: " << strerror(errno) << std::endl;
    }
}

RainStormMessage Manager::receiveMessageFromSocket(int sockfd){
    char buffer[4096] = {0};
    int bytesRead = 0;
    if ((bytesRead = recv(sockfd, buffer, sizeof(buffer), 0)) < 0) {
        std::cerr << "Error receiving message: " << strerror(errno) << std::endl;
    }
    buffer[bytesRead] = '\0';
    std::string serializedMessage(buffer);
    // cout<<"RECEIVED_MSG\n";
    // cout<<"----------------\n"<<serializedMessage<<"\n----------------\n";
    return RainStormMessage::deserializeRainStormMessage(serializedMessage);
}

int Manager::getStormSocketForNode(std::string nodeId){
    // cout<<"Getting storm socket for node: "<<nodeId<<endl;
    std::unique_lock<std::mutex> lock(nodeMapMutex);
    if(nodeIdToNode.find(nodeId) == nodeIdToNode.end()){
        if(nodeId == selfNode.getNodeId()){
            return getStormSocketForNode(selfNode);
        } else {
            std::cerr << "Node not found for node id: " << nodeId << std::endl;
            return -1;
        }
    } else {
        Node node = nodeIdToNode[nodeId];
        return getStormSocketForNode(node);
    }
    
}



