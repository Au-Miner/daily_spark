import requests
from threading import Thread


class MyThread(Thread):
    def __init__(self, name):  # 可以通过初始化来传递参数
        super(MyThread, self).__init__()
        self.name = name

    def run(self):  # 必须有的函数
        # for i in range(10):
        while True:
            url = "http://10.10.43.44:8080/api/group/userFailGroupPlan?userId=1&planId=31"
            headers = {
                'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIiwibmFtZSI6IkFkbWluIiwiZXhwIjoxNjUzMDU0NzE0LCJpYXQiOjE2NTI2MjI3MTR9.TuOaZZ2pn9IuqEh4GCKlKst3G8nO8VJdOLG7oD2KKKk'
            }
            response = requests.get(url=url, headers=headers)

        # print(response.status_code)
        # print(response.url)
        # print(response.headers)
        # print(response.text)


if __name__ == '__main__':
    t1 = MyThread("线程1")
    t2 = MyThread("线程2")
    t3 = MyThread("线程3")
    t4 = MyThread("线程4")
    t5 = MyThread("线程5")
    t6 = MyThread("线程6")
    t7 = MyThread("线程7")
    t8 = MyThread("线程8")
    t9 = MyThread("线程9")
    t10 = MyThread("线程10")
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()

