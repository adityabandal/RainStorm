#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include "RainStormMessage.h"
using namespace std;

class State {
    std::mutex mtx;

    unordered_map<string, vector<string>> nodeIdToTaskIds;
    unordered_map<string, string> taskIdToNodeId;
    unordered_map<string, string> taskToPrevTask;
    unordered_map<string, vector<string>> taskToTaskIds;
    unordered_map<string, RainStormMessage> taskToScheduleMessage;

public:
    State() {}
    ~State() {}

    void setScheduleMessageForTask(string taskName, RainStormMessage message) {
        std::lock_guard<std::mutex> lock(mtx);
        taskToScheduleMessage[taskName] = message;
    }

    RainStormMessage getScheduleMessageForTask(string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        return taskToScheduleMessage[taskName];
    }

    void addTaskId(string taskId, string nodeId) {
        std::lock_guard<std::mutex> lock(mtx);
        taskIdToNodeId[taskId] = nodeId;
        nodeIdToTaskIds[nodeId].push_back(taskId);
        taskToTaskIds[getTaskNameFromTaskId(taskId)].push_back(taskId);
    }

    unordered_map<int, pair<string, string>> getRoutingTableOfTask(string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        unordered_map<int, pair<string, string>> routingTable;
        vector<string> taskIds = taskToTaskIds[taskName];
        for(string taskId : taskIds) {
            string nodeId = taskIdToNodeId[taskId];
            int taskNumber = getTaskNumberFromTaskId(taskId);
            routingTable[taskNumber] = {nodeId, taskId};
        }
        return routingTable;
    }

    static int getTaskNumberFromTaskId(string taskId) {
        return stoi(taskId.substr(taskId.find("_") + 1));
    }

    static string getTaskNameFromTaskId(string taskId) {
        return taskId.substr(0, taskId.find("_"));
    }

    void addTaskChain(string prevTask, string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        taskToPrevTask[taskName] = prevTask;
    }

    vector<string> getTasksOnNode(string nodeId) {
        std::lock_guard<std::mutex> lock(mtx);
        return nodeIdToTaskIds[nodeId];
    }

    void updateTaskId(string taskId, string nodeId) {
        std::lock_guard<std::mutex> lock(mtx);
        string taskName = getTaskNameFromTaskId(taskId);
        nodeIdToTaskIds[nodeId].push_back(taskId);
        taskIdToNodeId[taskId] = nodeId;


    }

    string getPrevTask(string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        if(taskToPrevTask.find(taskName) != taskToPrevTask.end()){
            return taskToPrevTask[taskName];
        } else {
            return "";
        }
    }

    vector<pair<string, string>> getTaskIdNodeIdsForTask(string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        vector<string> taskIds = taskToTaskIds[taskName];
        vector<pair<string, string>> taskIdNodeIds;
        for(string taskId : taskIds) {
            string nodeId = taskIdToNodeId[taskId];
            taskIdNodeIds.push_back({taskId, nodeId});
        }
        return taskIdNodeIds;
    }

    int getNumberOfTasksOnNode(string nodeId) {
        std::lock_guard<std::mutex> lock(mtx);
        return nodeIdToTaskIds[nodeId].size();
    }

    vector<pair<string, string>> getTaskIdNodeIdPairForTask(string taskName) {
        std::lock_guard<std::mutex> lock(mtx);
        vector<string> taskIds = taskToTaskIds[taskName];
        cout<<"Task Ids size: "<<taskIds.size()<<endl;
        vector<pair<string, string>> taskIdNodeIds;
        for(string taskId : taskIds) {
            string nodeId = taskIdToNodeId[taskId];
            taskIdNodeIds.push_back({taskId, nodeId});
        }
        return taskIdNodeIds;
    }

    void deleteNode(string nodeId) {
        std::lock_guard<std::mutex> lock(mtx);
        nodeIdToTaskIds.erase(nodeId);
    }

    friend ostream& operator<<(ostream& os, const State& state) {
        os << "NodeId to TaskIds:\n";
        for (const auto& pair : state.nodeIdToTaskIds) {
            os << pair.first << ": ";
            for (const auto& taskId : pair.second) {
                os << taskId << " ";
            }
            os << "\n";
        }

        os << "TaskId to NodeId:\n";
        for (const auto& pair : state.taskIdToNodeId) {
            os << pair.first << ": " << pair.second << "\n";
        }

        os << "Task to PrevTask:\n";
        for (const auto& pair : state.taskToPrevTask) {
            os << pair.first << ": " << pair.second << "\n";
        }

        os << "Task to TaskIds:\n";
        for (const auto& pair : state.taskToTaskIds) {
            os << pair.first << ": ";
            for (const auto& taskId : pair.second) {
                os << taskId << " ";
            }
            os << "\n";
        }

        os << "Task to ScheduleMessage:\n";
        for (const auto& pair : state.taskToScheduleMessage) {
            os << pair.first << ": " << pair.second << "\n";
        }

        return os;
    }

};