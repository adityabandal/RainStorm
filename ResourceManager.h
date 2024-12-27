#ifndef RESOURCEMANAGER_H
#define RESOURCEMANAGER_H

#include "Node.h"
#include "HyDFS.h"
#include "Manager.h"
#include "State.h"
#include "Operations.h"
#include <iostream>
#include <unordered_map>
#include <vector>
#include <thread>
#include <utility>
using namespace std;
// #define RoutingTable unordered_map<int, pair<string, string>>

class ResourceManager : public Manager {
    State state;
    unordered_set<string> tasksToBeFailed;
    unordered_set<string> processed_outputs;
    


public:
    ResourceManager(char* port, Node _selfNode, HyDFS* _dfs);
    ~ResourceManager();

    string getNodeWithLeastJobs();
    void handleFailedNode(string nodeId);
    void updateRoutingTableForTask(string taskName);
    void messageHandler(int sockfd);
    void writeToFileStatefull(RainStormMessage message);
    void writeToFile(RainStormMessage message);

    void scheduleTask(string op1, bool isLastTask, int numTasks, string src_hydfs, string dest_hydfs, unordered_map<int, pair<string, string>> routingTable = {}, string param = "");
    void scheduleTask(RainStormMessage message);
    void executeCommand(string command) override;
    void startListening(int socket) override;
    void membershipUpdate(vector<Node> nodes) override;
    bool runCommandLine(string command) override;
};

#endif // RESOURCEMANAGER_H