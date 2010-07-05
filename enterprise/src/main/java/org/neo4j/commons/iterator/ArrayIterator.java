package org.neo4j.commons.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayIterator<T> implements Iterator<T>
{
    private final T[] array;
    private int index;

    public ArrayIterator( T[] array )
    {
        this.array = array;
    }

    public boolean hasNext()
    {
        return index < array.length;
    }

    public T next()
    {
        if ( !hasNext() ) throw new NoSuchElementException();
        return array[index++];
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
