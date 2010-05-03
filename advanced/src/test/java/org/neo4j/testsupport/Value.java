package org.neo4j.testsupport;

public final class Value<T>
{
    private T value;

    public Value()
    {
        this( null );
    }

    public Value( T value )
    {
        this.value = value;
    }

    public T set( T value )
    {
        try
        {
            return get();
        }
        finally
        {
            this.value = value;
        }
    }

    public T get()
    {
        return this.value;
    }
}
