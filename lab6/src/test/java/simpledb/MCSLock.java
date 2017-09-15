package simpledb;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by chauncy on 2017/9/15.
 */
public class MCSLock implements Lock {


    AtomicReference<MCSQNode> tail = new AtomicReference<>(new MCSQNode());
    ThreadLocal<MCSQNode> myNode;


    public MCSLock() {
        tail = new AtomicReference<>(new MCSQNode());
        myNode = new ThreadLocal<MCSQNode>() {
            @Override
            protected MCSQNode initialValue() {
                return new MCSQNode();
            }
        };
    }

    @Override
    public void lock() {
        MCSQNode mcsqNode = myNode.get();
        MCSQNode prev = tail.getAndSet(mcsqNode);
        if (prev != null){
            prev.next = mcsqNode;
        }
        while (mcsqNode.locked) ;
    }

    @Override
    public void unlock() {
        MCSQNode mcsqNode = myNode.get();
        if (mcsqNode.next == null) {
            if (tail.compareAndSet(mcsqNode,null)){
                return;
            }
            while (mcsqNode.next == null);
        }
        mcsqNode.next.locked = false;
        mcsqNode.next = null;
    }
}

class MCSQNode {
    volatile boolean locked;
    MCSQNode next;
}
