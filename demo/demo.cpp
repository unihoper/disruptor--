#include <vector>
#include <climits>
#include <cmath>
#include "disruptor/sequencer.h"
#include <iostream>
#include <thread>
#include <chrono>

typedef disruptor::Sequencer<int, 1<<16 > SequencerType;
typedef disruptor::SequenceBarrier<disruptor::BusySpinStrategy> SequenceBarrierType;
typedef std::array<int, 1<<16 > RingBuggerType;

std::array<int, 1<<16 > ringBuffer;
disruptor::Sequencer<int, 1<<16 > producer(ringBuffer);
std::vector<disruptor::Sequence*> consumers;
SequenceBarrierType* prodBarrier;

void powsum(double val, int pow, double& sum)
{
    sum += (pow == 1 ? val : (pow == 0 ? 1 : std::pow(val, pow)));
}
void writeAndRead(int round, int readercnt)
{  
    char msg[256];
    std::vector<double> psum(std::size_t(readercnt), 0.0);
    for(int i=0; i<round; ++i) {
        producer[i] = i;
        for(int j=0; j<readercnt; ++j) {
            powsum(producer[i], j+1, psum[j]);
        }
    }
    std::sprintf(msg, "writeAndRead cnt=%ld\n", round);
    std::cout << msg ;
    for(int i=0; i<readercnt; ++i) {
        std::cout << "psum[" <<i<<"]=" << psum[i] <<", ";
    }
    std::cout << std::endl;
}

void writeAndReadSync(int round, int readercnt)
{  
    char msg[256];
    int64_t widx = producer.GetCursor();
    int64_t count = 0;
    bool exit = false;
    std::vector<double> psum(std::size_t(readercnt), 0.0);
    do {
        widx = producer.Claim();
        producer[widx] = (count < round ? count : -1);
        producer.Publish(widx);
        ++count;
        int wcursor = producer.GetCursor();
        for(int i=0; i<readercnt; ++i) {
            auto &consumer = *(consumers[i]);
            int64_t ridx = consumer.sequence();
            while (ridx <wcursor) {
                ridx = consumer.IncrementAndGet(1);
                if (producer[ridx] < 0) {
                    exit = true;
                    break;
                }
                powsum(producer[ridx], i+1, psum[i]);
            }
        }
    } while(!exit);
    std::sprintf(msg, "writeAndReadSync cnt=%ld\n", round);
    std::cout << msg ;
    for(int i=0; i<readercnt; ++i) {
        std::cout << "psum[" <<i<<"]=" << psum[i] <<", ";
    }
    std::cout << std::endl;
}
void writer(int round)
{  
    char msg[256];
    int64_t bidx = producer.GetCursor();
    int64_t eidx = bidx + round;
    int64_t idx = bidx;
    do {
        idx = producer.Claim();
        producer[idx] = (idx <= eidx ? idx - bidx - 1: -1);
        producer.Publish(idx);
    } while(idx <= eidx);
    std::sprintf(msg, "writer cnt=%ld\n", round);
    std::cout << msg ;
}
void reader(disruptor::Sequence* consumer, int pow, double* sum)
{
    char msg[256];
    bool exit = false;
    int64_t idx = consumer->sequence();
    int64_t bidx = idx;
    do {
        int64_t cursor=producer.GetCursor();
        while(idx < cursor) {
            idx = consumer->IncrementAndGet(1);
            if (producer[idx]<0) {
                exit = true;
                break;
            }
            powsum(producer[idx], pow, *sum);
        }
    } while(!exit);
    std::sprintf(msg, "reader cnt=%ld\n", idx - bidx);
    std::cout << msg ;
}
void readerV1(disruptor::Sequence* consumer, int pow, double* sum)
{
    char msg[256];
    bool exit = false;
    int64_t idx = consumer->sequence();
    int64_t bidx = idx;
    do {
        int64_t cursor=prodBarrier->WaitFor(idx);
        while(!exit && idx < cursor) {
            ++idx;
            if (producer[idx]<0) {
                exit = true;
            } else {
                powsum(producer[idx], pow, *sum);
            }
            consumer->set_sequence(idx);
        }
    } while(!exit);
    std::sprintf(msg, "reader cnt=%ld\n", idx - bidx);
    std::cout << msg ;
}

void readerV2(disruptor::Sequence* consumer, int pow, double* sum)
{
    char msg[256];
    bool exit = false;
    int64_t idx = consumer->sequence();
    int64_t bidx = idx;
    do {
        int64_t cursor=prodBarrier->WaitFor(idx);
        while(!exit && idx < cursor) {
            ++idx;
            if (producer[idx]<0) {
                exit = true;
            } else {
                powsum(producer[idx], pow, *sum);
            }
            //consumer->set_sequence(idx);
        }
        consumer->set_sequence(idx);
    } while(!exit);
    std::sprintf(msg, "reader cnt=%ld\n", idx - bidx);
    std::cout << msg ;
}


int main(int argc, char** argv) 
{
    int round = 1 << 20;
    int readercnt = 2;
    if (argc > 1) {
        round = std::stoi(argv[1]);
    }
    if (argc > 2) {
        readercnt = std::stoi(argv[2]);
    }
    //initiate sequencer
    consumers.assign(readercnt, nullptr);
    for(int i=0; i<readercnt; ++i) {
        consumers[i] = new disruptor::Sequence();
    }
    producer.set_gating_sequences(consumers);

    std::cout << "Demo of disruptoer: round=" << round << ", reader=" << readercnt << std::endl;
    auto tstart = std::chrono::system_clock::now();
    writeAndRead(round, readercnt);
    auto tend = std::chrono::system_clock::now();
    auto ms = std::chrono::nanoseconds(tend - tstart);
    std::cout << "single thread:" << ms.count() << " ns, avg=" << ms.count() / (round + 1.0) << "ns/op" << std::endl;

    tstart = std::chrono::system_clock::now();
    writeAndReadSync(round, readercnt);
    tend = std::chrono::system_clock::now();
    ms = std::chrono::nanoseconds(tend - tstart);
    std::cout << "single thread sync:" << ms.count() << " ns, avg=" << ms.count() / (round + 1.0) << "ns/op" << std::endl;

    tstart = std::chrono::system_clock::now();
    std::vector<double> psum(std::size_t(readercnt), 0.0);
    std::thread  wth(writer, round);
    std::vector<std::thread> readers;
    for(int i=0; i<readercnt; ++i) {
        readers.emplace_back(reader, consumers[i], i+1, &(psum[i]));
    }
    wth.join();
    for(int i=0; i<readercnt; ++i) {
        readers[i].join();
    }
    tend = std::chrono::system_clock::now();
    ms = std::chrono::nanoseconds(tend - tstart);
    std::cout << "multithread thread:" << ms.count() << " ns, avg=" << ms.count() / (round + 1.0) << "ns/op" << std::endl;
    for(int i=0; i<readercnt; ++i) {
        std::cout << "psum[" <<i<<"]=" << psum[i] <<", ";
    }
    std::cout << std::endl;

    tstart = std::chrono::system_clock::now();
    std::vector<disruptor::Sequence*> dep(0);
    prodBarrier = producer.NewBarrier(dep);
    psum.assign(std::size_t(readercnt), 0.0);
    std::thread  wth1(writer, round);
    readers.clear();
    for(int i=0; i<readercnt; ++i) {
        readers.emplace_back(readerV1, consumers[i], i+1, &(psum[i]));
    }
    wth1.join();
    for(int i=0; i<readercnt; ++i) {
        readers[i].join();
    }
    tend = std::chrono::system_clock::now();
    ms = std::chrono::nanoseconds(tend - tstart);
    std::cout << "multithread thread V1:" << ms.count() << " ns, avg=" << ms.count() / (round + 1.0) << "ns/op" << std::endl;
    for(int i=0; i<readercnt; ++i) {
        std::cout << "psum[" <<i<<"]=" << psum[i] <<", ";
    }
    std::cout <<std::endl;
    delete prodBarrier;

    tstart = std::chrono::system_clock::now();
    prodBarrier = producer.NewBarrier(dep);
    psum.assign(std::size_t(readercnt), 0.0);
    std::thread  wth2(writer, round);
    readers.clear();
    for(int i=0; i<readercnt; ++i) {
        readers.emplace_back(readerV2, consumers[i], i+1, &(psum[i]));
    }
    wth2.join();
    for(int i=0; i<readercnt; ++i) {
        readers[i].join();
    }
    tend = std::chrono::system_clock::now();
    ms = std::chrono::nanoseconds(tend - tstart);
    std::cout << "multithread thread V2:" << ms.count() << " ns, avg=" << ms.count() / (round + 1.0) << "ns/op" << std::endl;
    for(int i=0; i<readercnt; ++i) {
        std::cout << "psum[" <<i<<"]=" << psum[i] <<", ";
    }
    std::cout <<std::endl;
    delete prodBarrier;
    
    for(int i=0; i<readercnt; ++i) {
        delete consumers[i];
    }
}
