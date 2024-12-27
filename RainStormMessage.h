#ifndef RAINSTORMMESSAGE_H
#define RAINSTORMMESSAGE_H


#include <iostream>
#include <tuple>
#define Tuple std::tuple<std::string, std::string, std::string>

enum RainStormMessageType {
    SCHEDULE,
    STARTED,
    UPDATE_ROUTING_TABLE,
    INPUT,
    INPUT_PROCESSED,
    COMMAND,
    OUTPUT,
    UNKNOWN
};

enum TaskType {
    SOURCE,
    FILTER,
    TRANSFORM,
    AGGREGATE
};

#include <unordered_map>
#include <vector>



class RainStormMessage {
    RainStormMessageType type;
    std::string command;
    std::string taskId;
    std::string taskName;
    TaskType taskType;
    bool isEndTask;
    std::string src_fileName;
    std::string dest_fileName;
    int startOffset;
    int endOffset;
    int startLineNumber;
    std::string param1;
    std::string leaderNodeId;
    Tuple tuple;
    std::string sourceNodeId;
    std::string sourceTaskId;
    std::unordered_map<int, std::pair<std::string, std::string>> routingTable; // <hash, <nodeId, taskId>>
    bool isToBeFailed;

    bool isStateFull;
    std::unordered_map<std::string, int> state;
    std::vector<Tuple> batchedOutputs;

    RainStormMessage(RainStormMessageType type) 
        : type(type), taskId(""), taskName(""), taskType(SOURCE), isEndTask(false), 
          src_fileName(""), dest_fileName(""), startOffset(0), endOffset(0), startLineNumber(0), isToBeFailed(false) {}

public:
    RainStormMessage() {}
    static const std::unordered_map<std::string, TaskType> taskNameToTaskTypeMap;
    // Getters
    RainStormMessageType getType() const { return type; }
    std::string getCommand() const { return command; }
    std::string getTaskId() const { return taskId; }
    std::string getTaskName() const { return taskName; }
    TaskType getTaskType() const { return taskType; }
    bool getIsEndTask() const { return isEndTask; }
    std::string getSrcFileName() const { return src_fileName; }
    std::string getDestFileName() const { return dest_fileName; }
    std::unordered_map<int, std::pair<std::string, std::string>> getRoutingTable() const { return routingTable; }
    int getStartOffset() const { return startOffset; }
    int getEndOffset() const { return endOffset; }
    int getStartLineNumber() const { return startLineNumber; }
    Tuple getTuple() const { return tuple; }
    std::string getParam1() const { return param1; }
    std::string getLeaderNodeId() const { return leaderNodeId; }
    std::string getSourceNodeId() const { return sourceNodeId; }
    std::string getSourceTaskId() const { return sourceTaskId; }
    bool getIsToBeFailed() const { return isToBeFailed; }
    bool getIsStateFull() const { return isStateFull; }
    std::unordered_map<std::string, int> getState() const { return state; }
    std::vector<Tuple> getBatchedOutputs() const { return batchedOutputs; }



    // Setters
    void setType(RainStormMessageType type) { this->type = type; }
    void setCommand(const std::string& command) { this->command = command; }
    void setTaskId(const std::string& taskId) { this->taskId = taskId; }
    void setTaskName(const std::string& taskName) { this->taskName = taskName; }
    void setTaskType(TaskType taskType) { this->taskType = taskType; }
    void setIsEndTask(bool isEndTask) { this->isEndTask = isEndTask; }
    void setSrcFileName(const std::string& src_fileName) { this->src_fileName = src_fileName; }
    void setDestFileName(const std::string& dest_fileName) { this->dest_fileName = dest_fileName; }
    void setRoutingTable(const std::unordered_map<int, std::pair<std::string, std::string>>& routingTable) { this->routingTable = routingTable; }
    void setStartOffset(int startOffset) { this->startOffset = startOffset; }
    void setEndOffset(int endOffset) { this->endOffset = endOffset; }
    void setStartLineNumber(int startLineNumber) { this->startLineNumber = startLineNumber; }
    void setTuple(const Tuple& tuple) { this->tuple = tuple; }
    void setParam1(const std::string& param1) { this->param1 = param1; }
    void setLeaderNodeId(const std::string& leaderNodeId) { this->leaderNodeId = leaderNodeId; }
    void setSourceNodeId(const std::string& sourceNodeId) { this->sourceNodeId = sourceNodeId; }
    void setSourceTaskId(const std::string& sourceTaskId) { this->sourceTaskId = sourceTaskId; }
    void setIsToBeFailed(bool isToBeFailed) { this->isToBeFailed = isToBeFailed; }
    void setIsStateFull(bool isStateFull) { this->isStateFull = isStateFull; }
    void setState(const std::unordered_map<std::string, int>& state) { this->state = state; }
    void setBatchedOutputs(const std::vector<Tuple>& batchedOutputs) { this->batchedOutputs = batchedOutputs; }
    


public:
    static RainStormMessage getRainStormMessage(RainStormMessageType type);
    static RainStormMessage deserializeRainStormMessage(const std::string& serialized);
    std::string serializeRainStormMessage();
    static RainStormMessageType stringToRainStormMessageType(std::string type);
    static std::string rainStormMessageTypeToString(RainStormMessageType type);
    static TaskType getTaskTypeFromName(const std::string& taskName);
    std::string getTypeString();

    friend std::ostream& operator<<(std::ostream& os, const RainStormMessage& message);
    RainStormMessage& operator=(const RainStormMessage& other);

    static std::string taskTypeToString(TaskType type);
    static TaskType stringToTaskType(std::string type);


    
};

#endif // RAINSTORMMESSAGE_H