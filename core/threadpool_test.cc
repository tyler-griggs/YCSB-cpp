// #include <iostream>
// #include "threadpool.h"

// // For sleeping test
// #include <chrono>
// #include <thread> 

// void* TestPrint() {
//   fprintf(stderr, "Success!\n");
//   return nullptr;
// }

// int main(void) {
//   ThreadPool tp;

//   tp.start(1);

//   std::function<void*()> f = &TestPrint;

//   tp.dispatch(f);
//   tp.dispatch(f);
//   tp.dispatch(f);

//   std::this_thread::sleep_for(std::chrono::seconds(1));
// }