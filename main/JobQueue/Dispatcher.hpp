#pragma once
#include "PriorityScheduler.hpp"
#include "WorkerPool.hpp"

class Dispatcher{
    public:
    Dispatcher(PriorityScheduler& scheduler,WorkerPool& pool){
        void start();
        void stop();
    }
    private:
        void run();
};