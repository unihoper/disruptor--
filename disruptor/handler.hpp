/** This file contains the key data structure of async publisher/receiver to assist using disruptor queue for inter-thread messaging. */
#ifndef __DISRUPTOR__HANDLER_HPP__
#define __DISRUPTOR__HANDLER_HPP__
#include "sequencer.h"

namespace disruptor {

/** Plain handler, defines interface */
template <class T>
class Handler {
public:
    using DATA_TYPE = T;
    virtual void process(const DATA_TYPE* pMD) noexcept {}
};

/** Sequential Handler */
template <class T>
class SequentialHandler : virtual public Handler<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    SequentialHandler(){}
    SequentialHandler(const std::vector<BASE_HANDLER_TYPE*>& chain_): chain(chain_) {}
    virtual BASE_HANDLER_TYPE* addHandler(BASE_HANDLER_TYPE* rcv) { chain.push_back(rcv); return rcv; }
    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* rcv) {
        for(auto it = chain.begin(); it != chain.end(); ++it) {
            if (*it == rcv) {
                chain.erase(it);
                return rcv;
            }
        }
        return nullptr;
    }
    virtual void process(const DATA_TYPE* pMD) noexcept override {
        for(auto& rcv : chain) {
            rcv->process(pMD);
        }
    }

protected:
    std::vector<BASE_HANDLER_TYPE* > chain;
};

/** Async receiver, receive message from a disruptor queue. */
template <class T, std::size_t N, typename C, typename W>
class AsyncHandler: public Handler<T>
{
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using SEQUENCER_TYPE = disruptor::Sequencer<T, N, C, W>;
    using BARRIER_TYPE = disruptor::SequenceBarrier<W>;
    using STOP_CHECK_FUNCTION_TYPE = std::function<bool(const DATA_TYPE*)>;
protected:
    BASE_HANDLER_TYPE* handler;
    disruptor::Sequence*  sequence;
    bool stopFlag;
    STOP_CHECK_FUNCTION_TYPE stopCheck;
    std::thread* work_thread;
    SEQUENCER_TYPE* sequencer;

public:
    AsyncHandler(BASE_HANDLER_TYPE* handler_)
    : handler(handler_), sequencer(nullptr)
    , stopFlag(true), stopCheck(nullptr), work_thread(nullptr)
    {
        sequence = new disruptor::Sequence();
        handler = handler_;
    }

    virtual ~AsyncHandler() {
        signal_stop();
        join();
        delete sequence;
    }

    void join() noexcept {
        if (work_thread) {
            work_thread->join();
            delete work_thread;
            work_thread = nullptr;
        }
    }
    void signal_stop() noexcept {
        if (work_thread) {
            stopFlag = true;
        }
    }
    void assign_stop_condition(const STOP_CHECK_FUNCTION_TYPE& stop_check_) {
        //can only assign once
        if (!stopCheck) stopCheck = stop_check_;
    }
    disruptor::Sequence* getSequence() noexcept { return sequence; }

    std::thread* attach(SEQUENCER_TYPE* sequencer_) noexcept {
        //create the work thread to start processing data
        if (work_thread) {
            signal_stop();
            join(); //wait
        }
        stopFlag = false;
        work_thread = new std::thread([this, sequencer_] { this->doWork(sequencer_); });
        sequencer = sequencer_;
    }
    std::thread* getWorkThread() noexcept { return work_thread; }

protected:
    void doWork(SEQUENCER_TYPE* sequencer_, int64_t init_idx = disruptor::kInitialCursorValue) noexcept {
        //the body of the async handler, it kept checking the sequencer and process data when new ones comes in
        std::vector<disruptor::Sequence*> dep(0);
        BARRIER_TYPE *barrier = sequencer_->NewBarrier(dep);
        sequence->set_sequence(init_idx);
        int64_t idx = init_idx;
        bool stopCondition = false; //dynamic check of stop condition
        do {
            int64_t cursor=barrier->WaitFor(idx);
            while(!stopFlag && idx < cursor) {
                ++idx;
                const DATA_TYPE* msg = &((*sequencer_)[idx]);
                if (stopCheck && stopCheck(msg)) {
                    stopCondition = true;
                    break;
                }
                handler->process(&((*sequencer_)[idx]));
            }
            sequence->set_sequence(idx);
        } while(!stopFlag && !stopCondition);
    }
};

template <class T, std::size_t N, typename C = disruptor::kDefaultClaimStrategyTemplate<N>, typename W = disruptor::kDefaultWaitStrategy>
class AsyncGateway: public Handler<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using SEQUENCER_TYPE = disruptor::Sequencer<T, N, C, W>;
    using HANDLER_TYPE = AsyncHandler<T, N, C, W>;
    using STOP_CHECK_FUNCTION_TYPE = std::function<bool(const DATA_TYPE*)>;

    AsyncGateway(const std::vector<BASE_HANDLER_TYPE *>& receivers_)
    : data_sequencer()
    {
        std::vector<disruptor::Sequence*> seq;
        for(auto it = receivers_.begin(); it != receivers_.end(); ++it) {
            HANDLER_TYPE* arcv = new HANDLER_TYPE(*it);
            receivers.emplace_back(arcv);
            seq.emplace_back(arcv->getSequence());
        }
        data_sequencer.set_gating_sequences(seq);
        for(auto & arcv : receivers) {
            arcv->attach(&data_sequencer);
        }
    }

    virtual ~AsyncGateway() {
        signal_stop_all();
        join_all();
        for(auto it=receivers.begin(); it != receivers.end(); ++it) {
            if (*it) {
                delete *it;
                *it = nullptr;
            }
        }
        receivers.clear();
    }

    virtual void process(const DATA_TYPE* pMD) noexcept override
    {
        int64_t idx = data_sequencer.Claim();
        std::memcpy(&(data_sequencer[idx]), pMD, sizeof(DATA_TYPE));
        data_sequencer.Publish(idx);
    }

    void signal_stop_all() noexcept {
        for(auto& rcv : receivers) {
            rcv->signal_stop();
        }
    }
    void join_all() noexcept {
        for(auto& rcv : receivers) {
            rcv->join();
        }
    }
    //note: do not use this yet, as a stopped async handler will not properly detach itself from the gateway and that will block the queue!
    void assign_stop_condition_all(const STOP_CHECK_FUNCTION_TYPE& func) {
        if (func) {
            for(auto& rcv : receivers) {
                rcv->assign_stop_condition(func);
            }
        }
    }
protected:
    SEQUENCER_TYPE data_sequencer;
    std::vector<HANDLER_TYPE* > receivers;
};

template <class T>
class CompositeHandler: public SequentialHandler<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using STOP_CHECK_FUNCTION_TYPE = std::function<bool(const DATA_TYPE*)>;

    CompositeHandler(){}
    ~CompositeHandler(){
        for(auto& rcv : derived) {
            if (rcv) delete rcv;
        }
        derived.clear();
    }

    /** connect each rcv async.(wrap with an AsyncHandler)  with an AsyncGateway(a newly spawn queue). */
    template<std::size_t N=1024ul, typename C = disruptor::kDefaultClaimStrategyTemplate<N>, typename W = disruptor::kDefaultWaitStrategy>
    AsyncGateway<T, N, C, W>* addHandlerAsync(const std::vector<BASE_HANDLER_TYPE *>& rcvs, const STOP_CHECK_FUNCTION_TYPE& stopCondition = STOP_CHECK_FUNCTION_TYPE(nullptr))
    {
        AsyncGateway<T, N, C, W> *asyncPub = new AsyncGateway<T, N, C, W>(rcvs);
        if (stopCondition) {
            asyncPub->assign_stop_condition_all(stopCondition);
        }
        SequentialHandler<T>::addHandler(asyncPub);
        derived.push_back(asyncPub);
        return asyncPub;
    }

    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* handler) override {
        BASE_HANDLER_TYPE* d = SequentialHandler<T>::removeHandler(handler);
        if (d) {
            //check if d is an internally derived handler to be removed
            for(auto it=derived.begin(); it != derived.end(); ++it) {
                if (*it == d) {
                    derived.erase(it);
                    delete d;
                    d = nullptr;
                    break;
                }
            }
        }
        return d;
    }
protected:
    std::vector<BASE_HANDLER_TYPE*> derived;
};
}
#endif
