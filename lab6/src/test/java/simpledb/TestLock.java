package simpledb;

/**
 * Created by chauncy on 2017/9/15.
 */
public class TestLock {


    public static void main(String[] args) {


//        testClH();
        testMSC();
    }


    public static void testMSC() {
        Lock clhLock = new MCSLock();
        for (int i = 0; i < 10; i++) {
            new MyThread("thread-" + i, clhLock, 10 - i).start();
        }

    }


    public static void testClH() {
        Lock clhLock = new CLHLock();
        for (int i = 0; i < 10; i++) {
            new MyThread("thread-" + i, clhLock, 10 - i).start();
        }

    }


    static class MyThread extends Thread {

        private Lock lock;
        private int anInt;

        public MyThread(String name, Lock lock, int a) {
            currentThread().setName(name);
            this.lock = lock;
            anInt = a;
        }

        @Override
        public void run() {

            doSomeThing();
        }

        private void doSomeThing() {
            lock.lock();
            System.out.println(currentThread().getName() + "get lock-----");
            lock.unlock();
        }
    }
}
