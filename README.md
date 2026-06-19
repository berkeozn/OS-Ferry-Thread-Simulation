# Multi-Threaded Ferry Simulation ⛴️

A system synchronization project written in C that simulates a ferry dock using POSIX threads, mutexes, and semaphores. This project demonstrates practical solutions to concurrency problems, avoiding busy-waiting while enforcing strict FIFO order for vehicles.

## Features
* **Concurrency Management:** Utilizes `pthread` library for creating isolated vehicle and ferry threads.
* **Strict Synchronization:** Implements semaphores (`sem_t`) for toll booths and condition variables (`pthread_cond_t`) for boarding queues.
* **Dynamic Configuration:** Reads simulation parameters (number of cars, trucks, minibuses, and ferry capacity) directly from `ayarlar.txt`.
* **Live Statistics:** Thread-safe calculation of max wait times, total loaded units, and ferry utilization ratios.

## How to Compile & Run
To run this simulation on a Unix/Linux environment:

1. Clone the repository:
   ```bash
   git clone [https://github.com/berkeozn/OS-Ferry-Thread-Simulation.git](https://github.com/berkeozn/OS-Ferry-Thread-Simulation.git)
   cd OS-Ferry-Thread-Simulation
   gcc main.c -lpthread -o ferry_sim
   ./ferry_sim
   CARS=5
MINIBUSES=3
TRUCKS=2
CAPACITY=10
