import time
from threading import Thread


class MyThread(Thread):
    def __init__(self, name):  # 可以通过初始化来传递参数
        super(MyThread, self).__init__()
        self.name = name

    def run(self):  # 必须有的函数
        print(f"{self.name}开始")
        time.sleep(0.2)
        print(f"{self.name}结束")


if __name__ == '__main__':
    t1 = MyThread("线程1")  # 创建第一个线程，并传递参数
    t2 = MyThread("线程2")  # 创建第二个线程，并传递参数
    t1.start()  # 开启第一个线程
    t2.start()  # 开启第二个线程
    print("主线程执行结束，子线程是依附于主线程存在的，所以，子线程都结束后，主线程才真正的结束。")


