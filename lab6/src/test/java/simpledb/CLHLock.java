package simpledb;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by chauncy on 2017/9/15.
 */
public class CLHLock implements Lock {
    @Override
    public void lock() {
       QNode my =  current.get();
       my.locked = true;
       QNode pre = tail.getAndSet(my);
       prev.set(pre);
       while (pre.locked);

    }

    @Override
    public void unlock() {
        QNode qNode = current.get();
        qNode.locked = false;
        current.set(prev.get());
    }


    AtomicReference<QNode> tail = new AtomicReference<>(new QNode());

    ThreadLocal<QNode> prev;
    ThreadLocal<QNode> current;


    public CLHLock() {
        tail = new AtomicReference<>(new QNode());
        prev = new ThreadLocal<QNode>() {
            @Override
            protected QNode initialValue() {
                return new QNode();
            }
        };
        current = new ThreadLocal<QNode>() {
            @Override
            protected QNode initialValue() {
                return new QNode();
            }
        };

    }
}

class QNode {
    public volatile boolean locked;
}