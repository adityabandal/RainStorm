#include "RainStormMessage.h"
#include <sstream>

const std::unordered_map<std::string, TaskType> RainStormMessage::taskNameToTaskTypeMap = {

    {"source", SOURCE},

    {"split", TRANSFORM},

    {"count", AGGREGATE},

    {"filter", FILTER},

    {"select", TRANSFORM},

    {"filterSignPost", TRANSFORM},

    {"countCategory", AGGREGATE}

};


RainStormMessage RainStormMessage::getRainStormMessage(RainStormMessageType type) {
    return RainStormMessage(type);
}

std::string RainStormMessage::rainStormMessageTypeToString(RainStormMessageType type) {
    switch (type) {
        case RainStormMessageType::SCHEDULE:
            return "SCHEDULE";
        case RainStormMessageType::STARTED:
            return "STARTED";
        case RainStormMessageType::UPDATE_ROUTING_TABLE:
            return "UPDATE_ROUTING_TABLE";
        case RainStormMessageType::INPUT:
            return "INPUT";
        case RainStormMessageType::INPUT_PROCESSED:
            return "INPUT_PROCESSED";
        case RainStormMessageType::COMMAND:
            return "COMMAND";
        case RainStormMessageType::OUTPUT:
            return "OUTPUT";
        default:
            return "UNKNOWN";
    }
}

RainStormMessageType RainStormMessage::stringToRainStormMessageType(std::string type) {
    if (type == "SCHEDULE") {
        return RainStormMessageType::SCHEDULE;
    } else if (type == "STARTED") {
        return RainStormMessageType::STARTED;
    } else if (type == "UPDATE_ROUTING_TABLE") {
        return RainStormMessageType::UPDATE_ROUTING_TABLE;
    } else if (type == "INPUT") {
        return RainStormMessageType::INPUT;
    } else if (type == "INPUT_PROCESSED") {
        return RainStormMessageType::INPUT_PROCESSED;
    } else if (type == "COMMAND") {
        return RainStormMessageType::COMMAND;
    } else if (type == "OUTPUT") {
        return RainStormMessageType::OUTPUT;
    }
    else {
        return RainStormMessageType::UNKNOWN;
    }
}

std::string RainStormMessage::serializeRainStormMessage() {
    std::string serialized = rainStormMessageTypeToString(type) + "\n";
    if (type == RainStormMessageType::COMMAND) {
        serialized += command + "\n";
        return serialized;
    } else if (type == RainStormMessageType::OUTPUT) {
        std::string str = isStateFull? "1" : "0";
        serialized += str + "\n";
        serialized += dest_fileName + "\n";
        if (isStateFull) {
            serialized += std::to_string(state.size()) + "\n";
            for (auto& pair : state) {
                serialized += pair.first + "\n";
                serialized += std::to_string(pair.second) + "\n";
            }
        } else {
            serialized += std::to_string(batchedOutputs.size()) + "\n";
            for (auto& tuple : batchedOutputs) {
                serialized += std::get<0>(tuple) + "\n";
                serialized += std::get<1>(tuple) + "\n";
                serialized += std::get<2>(tuple) + "\n";
            }
        }
        return serialized;
    }
    serialized += taskId + "\n";
    if(type == RainStormMessageType::SCHEDULE){
        serialized += taskName + "\n";
        serialized += std::to_string(taskType) + "\n";
        serialized += std::to_string(isEndTask) + "\n";
        serialized += src_fileName + "\n";
        serialized += dest_fileName + "\n";
        serialized += param1 + "\n";
        serialized += leaderNodeId + "\n";
        std::string str = isToBeFailed? "1" : "0";
        serialized += str + "\n";
        serialized += std::to_string(routingTable.size()) + "\n";
        if (taskType == TaskType::SOURCE) {
            serialized += std::to_string(startOffset) + "\n";
            serialized += std::to_string(endOffset) + "\n";
            serialized += std::to_string(startLineNumber) + "\n";
        }
        for (auto& pair : routingTable) {
            serialized += std::to_string(pair.first) + "\n";
            serialized += pair.second.first + "\n";
            serialized += pair.second.second + "\n";
        }
    } else if (type == RainStormMessageType::STARTED){
        serialized += "\n";
    } else if (type == RainStormMessageType::UPDATE_ROUTING_TABLE){
        for (auto& pair : routingTable) {
            serialized += std::to_string(pair.first) + "\n";
            serialized += pair.second.first + "\n";
            serialized += pair.second.second + "\n";
        }
    } else if (type == RainStormMessageType::INPUT || type == RainStormMessageType::INPUT_PROCESSED){
        serialized += std::get<0>(tuple) + "\n";
        serialized += std::get<1>(tuple) + "\n";
        serialized += std::get<2>(tuple) + "\n";
        serialized += sourceNodeId + "\n";
        serialized += sourceTaskId + "\n";
    }
    return serialized;
}

RainStormMessage RainStormMessage::deserializeRainStormMessage(const std::string& serialized) {

    std::istringstream iss(serialized);
    std::string line;
    std::getline(iss, line, '\n');
    RainStormMessageType type = stringToRainStormMessageType(line);
    RainStormMessage message = RainStormMessage::getRainStormMessage(type);
    if (type == RainStormMessageType::COMMAND) {
        std::getline(iss, message.command, '\n');
        return message;
    }
    if (type == RainStormMessageType::OUTPUT) {
        std::getline(iss, line, '\n');
        message.isStateFull = std::stoi(line);
        std::getline(iss, message.dest_fileName, '\n');
        if (message.isStateFull) {
            std::getline(iss, line, '\n');
            int stateSize = std::stoi(line);
            for (int i = 0; i < stateSize; i++) {
                std::getline(iss, line, '\n');
                std::string key = line;
                std::getline(iss, line, '\n');
                int value = std::stoi(line);
                message.state[key] = value;
            }
        } else {
            std::getline(iss, line, '\n');
            int batchSize = std::stoi(line);
            for (int i = 0; i < batchSize; i++) {
                std::getline(iss, line, '\n');
                std::string uniqueId = line;
                std::getline(iss, line, '\n');
                std::string key = line;
                std::getline(iss, line, '\n');
                std::string value = line;
                message.batchedOutputs.push_back(std::make_tuple(uniqueId, key, value));
            }
        }
        return message;
    }
    std::getline(iss, message.taskId, '\n');
    if(type == RainStormMessageType::SCHEDULE) {
        std::getline(iss, message.taskName, '\n');
        std::getline(iss, line, '\n');
        message.taskType = static_cast<TaskType>(std::stoi(line));
        std::getline(iss, line, '\n');
        message.isEndTask = std::stoi(line);
        std::getline(iss, message.src_fileName, '\n');
        std::getline(iss, message.dest_fileName, '\n');
        std::getline(iss, message.param1, '\n');
        std::getline(iss, message.leaderNodeId, '\n');
        std::getline(iss, line, '\n');
        message.isToBeFailed = std::stoi(line);
        std::getline(iss, line, '\n');
        int routingTableSize = std::stoi(line);
        if (message.taskType == TaskType::SOURCE) {
            std::getline(iss, line, '\n');
            message.startOffset = std::stoi(line);
            std::getline(iss, line, '\n');
            message.endOffset = std::stoi(line);
            std::getline(iss, line, '\n');
            message.startLineNumber = std::stoi(line);
        }
        for (int i = 0; i < routingTableSize; i++) {
            std::getline(iss, line, '\n');
            int hash = std::stoi(line);
            std::getline(iss, line, '\n');
            std::string nodeId = line;
            std::getline(iss, line, '\n');
            std::string taskId = line;
            message.routingTable[hash] = {nodeId, taskId};
        }
    } else if (type == RainStormMessageType::STARTED) {
        // Do nothing
    } else if (type == RainStormMessageType::UPDATE_ROUTING_TABLE) {
        while (std::getline(iss, line, '\n')) {
            int hash = std::stoi(line);
            std::getline(iss, line, '\n');
            std::string nodeId = line;
            std::getline(iss, line, '\n');
            std::string taskId = line;
            message.routingTable[hash] = {nodeId, taskId};
        }
    } else if (type == RainStormMessageType::INPUT || type == RainStormMessageType::INPUT_PROCESSED) {
        std::getline(iss, line, '\n');
        std::string key = line;
        std::getline(iss, line, '\n');
        std::string value = line;
        std::getline(iss, line, '\n');
        std::string timestamp = line;
        std::getline(iss, line, '\n');
        message.sourceNodeId = line;
        std::getline(iss, line, '\n');
        message.sourceTaskId = line;
        message.tuple = std::make_tuple(key, value, timestamp);
    }
    return message;
}

TaskType RainStormMessage::getTaskTypeFromName(const std::string& taskName) {
        return taskNameToTaskTypeMap.at(taskName);
}

std::ostream& operator<<(std::ostream& os, const RainStormMessage& message) {
    std::string taskType = "";
    if(RainStormMessage::taskNameToTaskTypeMap.find(message.taskName) != RainStormMessage::taskNameToTaskTypeMap.end()){
        taskType = RainStormMessage::taskTypeToString(RainStormMessage::taskNameToTaskTypeMap.at(message.taskName));
    }
    os << "Type: " << RainStormMessage::rainStormMessageTypeToString(message.type) << "\n"
        << "Task ID: " << message.taskId << "\n"
        << "Task Name: " << message.taskName << "\n"
        << "Task Type: " << taskType << "\n"
        << "Is End Task: " << (message.isEndTask ? "true" : "false") << "\n"
        << "Source File Name: " << message.src_fileName << "\n"
        << "Destination File Name: " << message.dest_fileName << "\n";
    if (message.taskType == TaskType::SOURCE) {
        os << "Start Offset: " << message.startOffset << "\n"
            << "End Offset: " << message.endOffset << "\n"
            << "Start Line Number: " << message.startLineNumber << "\n";
    }
    if (message.type == RainStormMessageType::INPUT || message.type == RainStormMessageType::INPUT_PROCESSED) {
        os << "Tuple: " << std::get<0>(message.tuple) << ", " << std::get<1>(message.tuple) << ", " << std::get<2>(message.tuple) << "\n";
        os << "Source Node ID: " << message.sourceNodeId << "\n";
        os << "Source Task ID: " << message.sourceTaskId << "\n";
    }
    os << "Routing Table: \n";
    for (const auto& entry : message.routingTable) {
        os << "  Hash: " << entry.first << " -> Node ID: " << entry.second.first << ", Task ID: " << entry.second.second << "\n";
    }
    return os;
}

std::string RainStormMessage::getTypeString() {
    return rainStormMessageTypeToString(type);
}

RainStormMessage& RainStormMessage::operator=(const RainStormMessage& other) {
    type = other.type;
    taskId = other.taskId;
    taskName = other.taskName;
    taskType = other.taskType;
    isEndTask = other.isEndTask;
    src_fileName = other.src_fileName;
    dest_fileName = other.dest_fileName;
    startOffset = other.startOffset;
    endOffset = other.endOffset;
    startLineNumber = other.startLineNumber;
    routingTable = other.routingTable;
    tuple = other.tuple;
    param1 = other.param1;
    sourceNodeId = other.sourceNodeId;
    sourceTaskId = other.sourceTaskId;
    return *this;
}

std::string RainStormMessage::taskTypeToString(TaskType type) {
    switch (type) {
        case TaskType::SOURCE:
            return "SOURCE";
        case TaskType::FILTER:
            return "FILTER";
        case TaskType::TRANSFORM:
            return "TRANSFORM";
        case TaskType::AGGREGATE:
            return "AGGREGATE";
        default:
            return "UNKNOWN";
    }   
}

TaskType RainStormMessage::stringToTaskType(std::string type) {
    if (type == "SOURCE") {
        return TaskType::SOURCE;
    } else if (type == "FILTER") {
        return TaskType::FILTER;
    } else if (type == "TRANSFORM") {
        return TaskType::TRANSFORM;
    } else if (type == "AGGREGATE") {
        return TaskType::AGGREGATE;
    } else {
        throw std::invalid_argument("Invalid task type");
    }
}
