/** This file contains the key data structure of async publisher/receiver to assist using disruptor queue for inter-thread messaging. */
/** Design map:
 *    We started with the concept of handler and distributor, distributor distribute a messge and handler process a message.
 *    A handler can be added to a distributor(subscribe) to process the published message.
 *    Then we can start thinking about expansions in several direction:
 *    1. one distributor could publish to multiple handlers in sequential orders(vanilla form, work in single threads)
 *    2. one distributor could publish to multiple handlers in parellel(work async.ly , relying on a queue)
 *    3. from here we could expand to any form of messaging acorss thread(or even machine) boundary.
 *
 *  So we will need a relayer structure.
 */
#ifndef __DISRUPTOR__HANDLER_HPP__
#define __DISRUPTOR__HANDLER_HPP__
#include "sequencer.h"
#include <memory>

namespace disruptor {

/** Plain handler, defines interface */
template <class T>
class Handler {
public:
    using DATA_TYPE = T;
    virtual void process(const DATA_TYPE* pMD) noexcept {}
    //for async distributor
    virtual void start() {}
    virtual void join() noexcept {}
    virtual void signal(int64_t stop_signal) noexcept {}
    virtual ~Handler() {}
};

/** Distributor base class */
template <class T>
class Distributor {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;

    virtual BASE_HANDLER_TYPE* addHandler(BASE_HANDLER_TYPE* rcv) { return nullptr; }
    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* rcv) { return nullptr; }
    virtual void distribute(const DATA_TYPE* pMD) noexcept {}
    //for async distributor
    virtual void start() {}
    virtual void join() noexcept {}
    virtual void signal(int64_t stop_signal) noexcept {}
    virtual ~Distributor() {}
};

/** Trivial connector to turn a given distributor into handler(and thus connect with other distributors) */
template <class T>
class Connector: virtual public Handler<T>{
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using BASE_DISTRIBUTOR_TYPE = Distributor<T>;

    Connector() {}
    Connector(BASE_DISTRIBUTOR_TYPE* dstr) : distributor(dstr) {}
    void setDistributor(BASE_DISTRIBUTOR_TYPE* dstr) { distributor.reset(dstr); }

    virtual ~Connector() { distributor.reset(); }
    virtual void process(const DATA_TYPE* pMD) noexcept override { if (distributor) distributor->distribute(pMD); }
    virtual void start() override { if (distributor) distributor->start(); }
    virtual void join() noexcept override { if (distributor) distributor->join(); }
    virtual void signal(int64_t stop_signal = disruptor::kDefaultStopSignal) noexcept override { if (distributor) distributor->signal(stop_signal); }

protected:
    std::unique_ptr<BASE_DISTRIBUTOR_TYPE> distributor{nullptr};
};

template <class T> Connector<T>* make_connector(Distributor<T>* dist) { return new Connector<T>(dist); }

/** Distributor that distributes to a single handler */
template <class T>
class SingleDistributor: virtual public Distributor<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;

    virtual BASE_HANDLER_TYPE* addHandler(BASE_HANDLER_TYPE* rcv) override { handler_ = rcv; return handler_; }
    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* rcv = nullptr) override {
        BASE_HANDLER_TYPE* res = handler_;
        handler_ = nullptr;
        return res;
    }

    virtual void distribute(const DATA_TYPE* pMD) noexcept override { if (handler_) handler_->process(pMD); }
    virtual void start() override { if (handler_) handler_->start(); }
    virtual void join() noexcept override { if (handler_) handler_->join(); }
    virtual void signal(int64_t stop_signal = kDefaultStopSignal) noexcept override { if (handler_) handler_->signal(stop_signal); }
    virtual ~SingleDistributor() {}
protected:
    BASE_HANDLER_TYPE *handler_ = nullptr;
};

/** Sequential Distributor: distribute to multiple handler in sequence3 */
template <class T>
class SequentialDistributor : virtual public Distributor<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    SequentialDistributor(){}
    SequentialDistributor(const std::vector<BASE_HANDLER_TYPE*>& chain_): chain(chain_) {}
    virtual ~SequentialDistributor() {}
    virtual BASE_HANDLER_TYPE* addHandler(BASE_HANDLER_TYPE* rcv) override {
        if (rcv == nullptr) return rcv;
        if (std::find(chain.begin(), chain.end(), rcv) == chain.end()) chain.push_back(rcv);
        return rcv;
    }
    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* rcv) override {
        for(auto it = chain.begin(); it != chain.end(); ++it) {
            if (*it == rcv) {
                chain.erase(it);
                rcv->join();
                return rcv;
            }
        }
        return nullptr;
    }
    virtual void distribute(const DATA_TYPE* pMD) noexcept override {
        for(auto& rcv : chain) {
            if (rcv) rcv->process(pMD);
        }
    }
    virtual void start() override {
        for(auto &handler : chain) {
            if (handler) handler->start();
        }
    }
    virtual void signal(int64_t stop_signal = kDefaultStopSignal) noexcept override {
        for(auto &handler : chain) {
            if (handler) handler->signal(stop_signal);
        }
    }
    virtual void join() noexcept override {
        for(auto &handler : chain) {
            if (handler) handler->join();
        }
    }


protected:
    std::vector<BASE_HANDLER_TYPE* > chain;
};


template <class T, std::size_t N, typename C = disruptor::kDefaultClaimStrategyTemplate<N>, typename W = disruptor::kDefaultWaitStrategy>
class ParallelDistributor : public Distributor<T> {
    /** Async receiver, receive message from a disruptor queue. */
    class AsyncHandlerWrapper
    {
    public:
        using DATA_TYPE = T;
        using BASE_HANDLER_TYPE = Handler<T>;
        using SEQUENCER_TYPE = disruptor::Sequencer<T, N, C, W>;
        using BARRIER_TYPE = disruptor::SequenceBarrier<W>;
    protected:
        BASE_HANDLER_TYPE* handler;
        std::atomic<bool> pauseFlag;
        std::atomic<int64_t> stopSequence;
        std::thread* work_thread;
        SEQUENCER_TYPE* sequencer;
        disruptor::Sequence*  sequence;
        std::chrono::nanoseconds timeout_interval{100000}; //timeout_interval check every 100us

    public:
        AsyncHandlerWrapper(BASE_HANDLER_TYPE* handler_)
        : handler(handler_)
        , pauseFlag(true)
        , stopSequence(disruptor::kDefaultStopSignal)
        , work_thread(nullptr)
        {
            sequence = new disruptor::Sequence();
        }

        virtual ~AsyncHandlerWrapper() {
            signal(kStopImmediatelySignal);
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
        void signal(int64_t stop_signal) noexcept {
            if (work_thread) {
                stopSequence.store(stop_signal, std::memory_order::memory_order_release);
            }
        }
        void signal_pause() noexcept {
            if (work_thread) {
                pauseFlag.store(true, std::memory_order::memory_order_release);
            }
        }
        void signal_resume() noexcept {
            if (work_thread) {
                pauseFlag.store(false, std::memory_order::memory_order_release);
            }
        }

        template <class R, class P>
        void set_time_out(const std::chrono::duration<R, P>& timeout_) { timeout_interval = timeout_; }
        void disable_timeout() { timeout_interval = 0; }

        disruptor::Sequence* getSequence() noexcept { return sequence; }

        std::thread* attach(SEQUENCER_TYPE* sequencer_) noexcept {
            //create the work thread to start processing data
            if (work_thread) {
                signal(kStopImmediatelySignal);
                join(); //wait
            }
            pauseFlag.store(false, std::memory_order::memory_order_release);
            stopSequence.store(kDefaultStopSignal, std::memory_order::memory_order_release);
            work_thread = new std::thread([this, sequencer_] { this->doWork(sequencer_); });
            sequencer = sequencer_;
            return work_thread;
        }
        std::thread* getWorkThread() noexcept { return work_thread; }

    protected:
        void doWork(SEQUENCER_TYPE* sequencer_, int64_t init_idx = disruptor::kInitialCursorValue) noexcept {
            //the body of the async handler, it kept checking the sequencer and process data when new ones comes in
            std::vector<disruptor::Sequence*> dep(0);
            BARRIER_TYPE *barrier = sequencer_->NewBarrier(dep);
            sequence->set_sequence(init_idx);
            int64_t idx = init_idx;
            int64_t stopIdx = kDefaultStopSignal;
            do {
                //allow timeout_interval to make sure we can stop even if there's no new publication
                if (stopIdx == kDefaultStopSignal) {
                    do{
                        stopIdx = stopSequence.load(std::memory_order::memory_order_acquire);
                        if (stopIdx != kDefaultStopSignal) break;
                    } while(pauseFlag.load(std::memory_order::memory_order_acquire));
                    if (stopIdx == kStopImmediatelySignal) break;
                }
                int64_t cursor=(timeout_interval <= std::chrono::nanoseconds(0) ? barrier->WaitFor(idx) : barrier->WaitFor(idx, timeout_interval));
                while(idx < cursor) {
                    ++idx;
                    const DATA_TYPE* msg = &((*sequencer_)[idx]);
                    handler->process(msg);
                }
                sequence->set_sequence(idx);
                if (stopIdx != kDefaultStopSignal && idx >= stopIdx) break;
            } while(true);
        }
    };

public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using SEQUENCER_TYPE = disruptor::Sequencer<T, N, C, W>;
    using STOP_CHECK_FUNCTION_TYPE = std::function<bool(const DATA_TYPE*)>;

    ParallelDistributor()
    : data_sequencer()
    { }

    virtual ~ParallelDistributor() {
        for(auto it=receivers.begin(); it != receivers.end(); ++it) {
            if (*it) {
                delete *it;
                *it = nullptr;
            }
        }
        receivers.clear();
    }

    virtual BASE_HANDLER_TYPE* addHandler(BASE_HANDLER_TYPE* rcv) override {
        if (started_ || rcv == nullptr) return nullptr;
        if (std::find(chain.begin(), chain.end(), rcv) == chain.end()) chain.push_back(rcv);
        return rcv;
    }
    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* rcv) override {
        if (started_) return nullptr;
        for(auto it = chain.begin(); it != chain.end(); ++it) {
            if (*it == rcv) {
                chain.erase(it);
                return rcv;
            }
        }
        return nullptr;
    }
    virtual void join() noexcept override {
        if (started_) {
            for(auto& rcv : receivers) {
                rcv->join();
            }
            started_ = false;
        }
    }
    //REMAIN: start right now is initialize + start, could separate to make it more dynamic
    virtual void start() override {
        if (!started_) {
            std::vector<disruptor::Sequence*> seq;
            for(auto &handler : chain) {
                AsyncHandlerWrapper* arcv = new AsyncHandlerWrapper(handler);
                receivers.emplace_back(arcv);
                seq.emplace_back(arcv->getSequence());
            }
            data_sequencer.set_gating_sequences(seq);
            for(auto & arcv : receivers) {
                arcv->attach(&data_sequencer);
            }
            started_ = true;
        }
    }

    //REMAIN: stop right now is pause + dispose, could separate to make it more dynamic
    virtual void signal(int64_t stop_signal = kDefaultStopSignal) noexcept override {
        if (started_) {
            int64_t signal = (stop_signal == kDefaultStopSignal ? last_claimed_idx : stop_signal);
            for(auto& rcv : receivers) {
                rcv->signal(signal);
            }
        }
    }

    virtual void distribute(const DATA_TYPE* pMD) noexcept override
    {
        if (!started_) return; //discard any data that came in before we are started
        last_claimed_idx = data_sequencer.Claim();
        std::memcpy(&(data_sequencer[last_claimed_idx]), pMD, sizeof(DATA_TYPE));
        data_sequencer.Publish(last_claimed_idx);
    }

    void signal_pause_all() noexcept {
        if (started_) {
            for(auto& rcv : receivers) {
                rcv->signal_pause();
            }
        }
    }
    void signal_resume_all() noexcept {
        if (started_) {
            for(auto& rcv : receivers) {
                rcv->signal_resume();
            }
        }
    }
protected:
    bool started_ = false; //no change to the chain once we've started the distribution(so we don't need to handle syncronization issue
    int64_t last_claimed_idx = disruptor::kInitialCursorValue;
    std::vector<BASE_HANDLER_TYPE* > chain;
    SEQUENCER_TYPE data_sequencer;
    std::vector<AsyncHandlerWrapper* > receivers;

};

template <class T>
class CompositeDistributor: public SequentialDistributor<T> {
public:
    using DATA_TYPE = T;
    using BASE_HANDLER_TYPE = Handler<T>;
    using STOP_CHECK_FUNCTION_TYPE = std::function<bool(const DATA_TYPE*)>;

    CompositeDistributor(){}
    virtual ~CompositeDistributor(){
        for(auto& rcv : derived) {
            if (rcv) delete rcv;
        }
        derived.clear();
    }
    virtual void signal(int64_t stop_signal = kDefaultStopSignal) noexcept override { SequentialDistributor<T>::signal(stop_signal); }
    /** connect each rcv async.(wrap with an AsyncHandler)  with an AsyncGateway(a newly spawn queue). */
    template<std::size_t N=1024ul, typename C = disruptor::kDefaultClaimStrategyTemplate<N>, typename W = disruptor::kDefaultWaitStrategy>
    BASE_HANDLER_TYPE* addAsyncHandlerParellel(const std::vector<BASE_HANDLER_TYPE *>& rcvs)
    {
        ParallelDistributor<T, N, C, W>* pd = new ParallelDistributor<T, N, C, W>();
        for(auto& rcv : rcvs) pd->addHandler(rcv);
        BASE_HANDLER_TYPE* res = this->addHandler(make_connector(pd));
        derived.push_back(res);
        return res;
    }
    template<std::size_t N=1024ul, typename C = disruptor::kDefaultClaimStrategyTemplate<N>, typename W = disruptor::kDefaultWaitStrategy>
    BASE_HANDLER_TYPE* addAsyncHandlerSequential(const std::vector<BASE_HANDLER_TYPE *>& rcvs)
    {
        SequentialDistributor<T> *sd = new SequentialDistributor<T>();
        for(auto& rcv : rcvs) sd->addHandler(rcv);
        ParallelDistributor<T, N, C, W>* pd = new ParallelDistributor<T, N, C, W>();
        pd->addHandler(make_connector(sd));
        BASE_HANDLER_TYPE* res = this->addHandler(make_connector(pd));
        derived.push_back(res);
        return res;
    }

    virtual BASE_HANDLER_TYPE* removeHandler(BASE_HANDLER_TYPE* handler) override {
        BASE_HANDLER_TYPE* d = this->removeHandler(handler);
        if (d) {
            //check if d is an internally derived handler to be removed
            for(auto it=derived.begin(); it != derived.end(); ++it) {
                if (*it == d) {
                    derived.erase(it);
                    d->signal(kStopImmediatelySignal);
                    d->join();
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
