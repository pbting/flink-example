package ifrat.com.flink.examples.alg;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MultiInteractivePrint {

    private static ReentrantLock lock = new ReentrantLock();
    private static Condition odd = lock.newCondition();
    private static Condition even = lock.newCondition();

    private static int value = 1;

    public static void main(String[] args) throws InterruptedException {

        Thread oddThread = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    while (value <= 100) {
                        if (MultiInteractivePrint.value % 2 != 0) {
                            //
                            System.err.println("odd: " + (value++));
                            even.signal();
                        } else {
                            try {
                                odd.await();
                            } catch (InterruptedException e) {
                                // nothing
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }

            }
        });


        Thread evenThread = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();

                try {
                    while (value <= 100) {
                        if (MultiInteractivePrint.value % 2 != 0) {
                            //
                            try {
                                even.await();
                            } catch (InterruptedException e) {
                                //
                            }
                        } else {
                            System.err.println("=> even: " + (value++));
                            odd.signal();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        });

        oddThread.start();
        evenThread.start();

        oddThread.join();
        evenThread.join();

        System.err.println("end");
    }
}
