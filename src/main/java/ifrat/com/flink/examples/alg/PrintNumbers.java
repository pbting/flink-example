package ifrat.com.flink.examples.alg;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PrintNumbers {
    private int number = 1;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private int threshold = 100;

    public void printOdd() {
        lock.lock();
        try {
            while (number <= threshold) {
                if (number % 2 == 0) {
                    condition.await();
                } else {
                    System.err.println("Odd: " + number);
                    number++;
                    condition.signalAll();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    public void printEven() {
        lock.lock();
        try {
            while (number <= threshold) {
                if (number % 2 != 0) {
                    condition.await();
                } else {
                    System.out.println("Even: " + number);
                    number++;
                    condition.signalAll();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        PrintNumbers printNumbers = new PrintNumbers();

        Thread oddThread = new Thread(printNumbers::printOdd);
        Thread evenThread = new Thread(printNumbers::printEven);

        oddThread.start();
        evenThread.start();

        try {
            oddThread.join();
            evenThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}