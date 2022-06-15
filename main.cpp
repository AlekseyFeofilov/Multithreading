#include <iostream>

#include <mutex>
#include <thread>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>

#include <vector>
#include <map>
#include <random>
#include <algorithm>

#include <cstdio>
#include <windows.h>

#pragma clang diagnostic push
#pragma ide diagnostic ignored "EndlessLoop"
using namespace std;

const short statusNumber = 2;
const short transferNumber = 3;
const int maxGenerateDelay = 1000;
const int statisticFrequency = 2000;

bool terminated = false;
condition_variable terminatedCV;
mutex terminatedMutex;

enum class Status {
    BUY, SELL
};

enum class Transfer {
    SMALL, MIDDLE, LARGE
};

class Person {
public:
    Transfer transfer;
    Status status;

    Person() = default;

    Person(Transfer transfer, Status status);

    bool operator==(Person &person) const;

    bool operator!=(Person &person) const;

    int transferCount() const;
};

bool Person::operator==(Person &person) const {
    return transfer == person.transfer && status == person.status;
}

bool Person::operator!=(Person &person) const {
    return transfer != person.transfer || status != person.status;
}

Person::Person(Transfer transfer, Status status) {
    this->transfer = transfer;
    this->status = status;
}

int Person::transferCount() const {
    switch (transfer) {
        case Transfer::SMALL:
            return 2;

        case Transfer::MIDDLE:
            return 6;

        case Transfer::LARGE:
            return 12;

        default:
            throw out_of_range("this transfer isn't exist (Person::transferCount)");
    }
}

class Bank {
    class CashBoxData {
    public:
        chrono::steady_clock::time_point lastCheckTime;
        chrono::microseconds activeTime{};
        chrono::microseconds inactiveTime{};

        Person to{};
        vector<Person> from;
        Person booking{};

        int currentCheck;
        int currentSum;

        CashBoxData();

        void reset();
    };

    int queueOpacity;
    vector<Person> queue;
    map<const thread::id, CashBoxData> cashBoxData;

    bool workday;
    bool proceed;
    bool needUpdate;

    atomic<unsigned int> deadEndCounter{};
    atomic<unsigned int> cashBoxCounter{};
    atomic<unsigned long long> served{};
    atomic<unsigned long long> canceled{};

    condition_variable clearCV;
    condition_variable_any workdayCV;
    condition_variable_any unfreezeCV;
    condition_variable_any updateCV;

    mutable shared_timed_mutex RWMutex;
    mutable shared_timed_mutex freezeMutex;
    mutable shared_timed_mutex updateMutex;
    mutable mutex mainMutex;
    mutable mutex clearMutex;

    random_device rd{};

    void continueWorkday();

    void continueCashBoxJob();

    Person generatePerson();

    void freezeWorkStatus();

    void unfreezeWorkStatus();

    void freezeWork();

    void waitUpdate();

    void pushBack(Person person);

    bool pop(int index);

    bool bookFirst();

    void updateCashBoxesData(int update);

    bool checkPerson(CashBoxData *data);

    CashBoxData *getCashBoxData();

    void complete();

    void checkFreezing();

    chrono::milliseconds getTimeDelay();

    void waitForStart();

    bool freeze_lock(const function<bool()> &body);

    void work(const function<void()> &body);

    void deadEnd(CashBoxData *data);

    void endCashBoxJob();

public:

    explicit Bank(int queueOpacity);

    void beginCashBoxJob();

    void beginWorkday();

    void endWorkday();

    void printStatistic();

    void clear();
};

Bank::CashBoxData::CashBoxData() {
    lastCheckTime = chrono::steady_clock::now();
    activeTime = chrono::microseconds(0);
    inactiveTime = chrono::microseconds(0);
    currentCheck = 0;
    currentSum = 0;
}

void Bank::CashBoxData::reset() {
    currentCheck = 0;
    currentSum = 0;
    from.clear();
}

Bank::Bank(int queueOpacity) {
    this->queueOpacity = queueOpacity;

    workday = false;
    proceed = true;
    needUpdate = false;

    deadEndCounter.store(0);
    cashBoxCounter.store(0);
    served.store(0);
    canceled.store(0);
}

bool Bank::freeze_lock(const function<bool()> &body) {
    bool complete;

    unique_lock<mutex> mainLock(mainMutex);
    freezeWorkStatus();
    unique_lock<shared_timed_mutex> lock(RWMutex);
    complete = body();
    unfreezeWorkStatus();

    return complete;
}

void Bank::work(const function<void()> &body) {
    waitForStart();

    while (workday) {
        body();
    }
}

void Bank::pushBack(Person person) {
    freeze_lock([this, &person] {
        queue.push_back(person);
        return true;
    });
}

bool Bank::checkPerson(CashBoxData *data) {
    shared_lock<shared_timed_mutex> lock(RWMutex);

    if (data->currentCheck >= queue.size()) return false;
    Person currentPerson = queue[data->currentCheck];

    if (currentPerson.status == data->to.status) return false;
    if (currentPerson.transfer > data->to.transfer) return false;
    data->booking = currentPerson;
    return true;
}

bool Bank::pop(int index) {
    return freeze_lock([this, index] {
        CashBoxData *data = getCashBoxData();
        if (index >= queue.size()) return false;
        if (queue[index] != data->booking) return false;

        Person person = queue[index];
        queue.erase(queue.cbegin() + index);

        data->from.push_back(person);
        data->currentSum += person.transferCount();

        updateCashBoxesData(index);
        return true;
    });
}

bool Bank::bookFirst() {
    return freeze_lock([this] {
        if (queue.empty()) return false;

        getCashBoxData()->to = queue[0];
        queue.erase(queue.cbegin());

        return true;
    });
}

void Bank::clear() {
    work([this] {
        unique_lock<mutex> clearLock(clearMutex);
        clearCV.wait(clearLock, [this] { return queue.size() >= queueOpacity || !workday; });

        freeze_lock([this] {
            canceled.fetch_add(queue.size());
            queue.clear();
            return true;
        });
    });

    cout << "clearing thread terminated" << endl;
}

void Bank::beginCashBoxJob() {
    cashBoxCounter.fetch_add(1);

    cashBoxData[this_thread::get_id()] = CashBoxData();
    continueCashBoxJob();
}

void Bank::continueCashBoxJob() {
    CashBoxData *data = getCashBoxData();

    work([this, &data] {
        checkFreezing();

        if (!bookFirst()) {
            waitUpdate();
            return;
        }

        while (data->currentSum < data->to.transferCount()) {
            if(!workday){
                break;
            }

            checkFreezing();

            if (data->currentCheck > queue.size()) {
                deadEnd(data);
            }

            if (checkPerson(data)) {
                pop(data->currentCheck);
            }

            data->currentCheck++;
        }

        if(data->currentSum >= data->to.transferCount()) {
            complete();
        }
    });

    endCashBoxJob();
}

void Bank::endCashBoxJob() {
    cout << "cashBox(thread) " << this_thread::get_id() << " terminated" << endl;
    cashBoxCounter.fetch_sub(1);
}

void Bank::deadEnd(CashBoxData *data){
    for (auto person: data->from) {
        pushBack(person);
    }

    data->reset();
    deadEndCounter.fetch_add(1);
    needUpdate = true;
    waitUpdate();
    deadEndCounter.fetch_sub(1);
}

void Bank::beginWorkday() {
    workday = true;
    workdayCV.notify_all();

    continueWorkday();
}

void Bank::continueWorkday() {
    while (workday) {
        this_thread::sleep_for(chrono::milliseconds(rd() % maxGenerateDelay));

        if (queue.size() < queueOpacity) {
            Person newPerson = generatePerson();
            pushBack(newPerson);
            needUpdate = false;
            updateCV.notify_all();
        } else if (deadEndCounter == cashBoxCounter) {
            clearCV.notify_one();
        }
    }

    cout << "generating thread terminated" << endl;
}

void Bank::endWorkday() {
    workday = false;

    while(cashBoxCounter > 0) {
        unfreezeCV.notify_all();
        needUpdate = false;
        updateCV.notify_all();
    }

    clearCV.notify_one();
}

Person Bank::generatePerson() {
    Person person{};

    switch (rd() % transferNumber) {
        case 0:
            person.transfer = Transfer::SMALL;
            break;

        case 1:
            person.transfer = Transfer::MIDDLE;
            break;

        case 2:
            person.transfer = Transfer::LARGE;
            break;

        default:
            throw out_of_range("this transfer isn't exist (Bank::generatePerson)");
    }

    switch (rd() % statusNumber) {
        case 0:
            person.status = Status::BUY;
            break;

        case 1:
            person.status = Status::SELL;
            break;

        default:
            throw out_of_range("this status isn't exist (Bank::generatePerson)");
    }

    return person;
}

void Bank::freezeWorkStatus() {
    proceed = false;
}

void Bank::unfreezeWorkStatus() {
    proceed = true;
    unfreezeCV.notify_all();
}

void Bank::checkFreezing() {
    if (!proceed) {
        freezeWork();
    }
}

chrono::milliseconds Bank::getTimeDelay() {
    CashBoxData *data = getCashBoxData();

    chrono::milliseconds answer = chrono::duration_cast<chrono::milliseconds>(
            chrono::steady_clock::now() - data->lastCheckTime);

    data->lastCheckTime = chrono::steady_clock::now();
    return answer;
}

void Bank::freezeWork() {
    CashBoxData *data = getCashBoxData();

    data->activeTime += getTimeDelay();

    shared_lock<shared_timed_mutex> lock(freezeMutex);
    unfreezeCV.wait(lock, [this] { return proceed; });

    data->inactiveTime += getTimeDelay();
}

void Bank::waitUpdate() {
    CashBoxData *data = getCashBoxData();

    data->activeTime += getTimeDelay();

    shared_lock<shared_timed_mutex> lock(updateMutex);
    updateCV.wait(lock, [this] { return !needUpdate; });

    data->inactiveTime += getTimeDelay();
}

void Bank::complete() {
    CashBoxData *data = getCashBoxData();

    sort(
            data->from.begin(),
            data->from.end(),
            [](Person a, Person b) { return a.transferCount() > b.transferCount(); }
    );

    int remainder = data->to.transferCount();
    int lastPerson = 0;

    while (remainder) {
        remainder -= data->from[lastPerson].transferCount();
        lastPerson++;
    }

    for (; lastPerson < data->from.size(); lastPerson++) {
        pushBack(data->from[lastPerson]);
    }

    data->reset();
    served.fetch_add(1);
    data->activeTime += getTimeDelay();
}

void Bank::updateCashBoxesData(int update) {
    for (auto data: cashBoxData) {
        if (data.second.currentCheck > update) {
            data.second.currentCheck--;
        }
    }
}

Bank::CashBoxData *Bank::getCashBoxData() {
    return &cashBoxData[this_thread::get_id()];
}

void Bank::printStatistic() {
    work([this] {
        cout << "served: " << served << endl;
        cout << "in queue: " << queue.size() << endl;
        cout << "canceled: " << canceled << endl;

        for (const auto &data: cashBoxData) {
            cout << "cashBox (thread): " << data.first;
            cout << " in work " << data.second.activeTime.count() / 100000;
            cout << ", wait " << data.second.inactiveTime.count() / 100000;
            cout << ". Relevant on: ";
            cout << chrono::system_clock::to_time_t(
                    chrono::system_clock::now() + (data.second.lastCheckTime - chrono::steady_clock::now()));
            cout << endl;
        }

        cout << endl << endl;
        this_thread::sleep_for(chrono::milliseconds(statisticFrequency));
    });

    cout << "statistic thread terminated" << endl;
}

void Bank::waitForStart() {
    if (!workday) {
        shared_lock<shared_timed_mutex> lock(RWMutex);
        workdayCV.wait(lock, [this] { return workday; });
    }
}

BOOL WINAPI consoleControlHandler(DWORD dwCtrlType) {
    if (dwCtrlType == CTRL_C_EVENT) {
        terminated = true;
        terminatedCV.notify_one();
        cout << "terminated in process" << endl;
        return TRUE;
    }

    return FALSE;
}

int main() {
    freopen("input.txt", "r", stdin);

    int queueOpacity, cashBoxNumber;
    cin >> queueOpacity >> cashBoxNumber;

    Bank bank(queueOpacity);
    thread cleaningThread(&Bank::clear, &bank);
    thread generatingThread(&Bank::beginWorkday, &bank);
    thread statisticThread(&Bank::printStatistic, &bank);
    vector<thread> cashBoxes;

    for (int i = 0; i < cashBoxNumber; i++) {
        cashBoxes.emplace_back(&Bank::beginCashBoxJob, &bank);
    }

    auto terminate = [&bank]() {
        unique_lock<mutex> lock(terminatedMutex);
        terminatedCV.wait(lock, [] { return terminated; });

        bank.endWorkday();
    };

    thread terminatedThread(terminate);

    SetConsoleCtrlHandler(consoleControlHandler, TRUE);

    cleaningThread.join();
    generatingThread.join();
    statisticThread.join();
    terminatedThread.join();

    for (auto &cashBox: cashBoxes) {
        cashBox.join();
    }

    cout << endl << "!!!terminated correct!!!";
    return 0;
}

#pragma clang diagnostic pop