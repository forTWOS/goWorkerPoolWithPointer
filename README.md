# goWorkerPoolWithPointer

golang，协程池测试代码
对比测试：
1.使用指针比不使用，更省内存
2.协程较优数量，与cpu相关，非越多越好——有可能与我这个模型有关，通道队列长度，影响效率
