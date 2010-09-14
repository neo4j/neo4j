package org.neo4j.helpers.collection;

import java.util.Iterator;


/**
 * Decorator class that wraps any iterator and remembers the current node.
 */

public class PositionedIterator<T> implements Iterator<T> {
    private Iterator<? extends T> inner;
    private T current;
    private Boolean initiated = false;

    /**
     * Creates an instance of the class, wrapping iterator
     * @param iterator The iterator to wrap
     */
    public PositionedIterator(Iterator<? extends T> iterator) {
        inner = iterator;
    }

    public boolean hasNext() {
        return inner.hasNext();
    }

    public T next() {
        initiated = true;
        current = inner.next();
        return current;
    }

    public void remove() {
        inner.remove();
    }

    /**
     * Returns the current node. Any subsequent calls to current will return the same object,
     * unless the next() method has been called.
     * @return The current node.
     */
    public T current() {
        if(!initiated)
            return next();

        return current;
    }
}
