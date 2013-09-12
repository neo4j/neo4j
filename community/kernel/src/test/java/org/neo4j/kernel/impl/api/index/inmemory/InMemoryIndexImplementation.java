package org.neo4j.kernel.impl.api.index.inmemory;

import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

abstract class InMemoryIndexImplementation implements IndexReader
{
    abstract void clear();

    @Override
    public final PrimitiveLongIterator lookup( Object value )
    {
        return doLookup( encode( value ) );
    }

    final void add( long nodeId, Object propertyValue )
    {
        doAdd( encode( propertyValue ), nodeId );
    }

    final void remove( long nodeId, Object propertyValue )
    {
        doRemove( encode( propertyValue ), nodeId );
    }

    abstract PrimitiveLongIterator doLookup( Object propertyValue );

    abstract void doAdd( Object propertyValue, long nodeId );

    abstract void doRemove( Object propertyValue, long nodeId );

    @Override
    public void close()
    {
    }

    private static Object encode( Object propertyValue )
    {
        if ( propertyValue instanceof Number )
        {
            return ((Number) propertyValue).doubleValue();
        }

        if ( propertyValue instanceof Character )
        {
            return propertyValue.toString();
        }

        if ( propertyValue.getClass().isArray() )
        {
            return new ArrayKey( ArrayEncoder.encode( propertyValue ) );
        }

        return propertyValue;
    }

    private static class ArrayKey
    {
        private final String arrayValue;

        private ArrayKey( String arrayValue )
        {
            this.arrayValue = arrayValue;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            ArrayKey other = (ArrayKey) o;

            return other.arrayValue.equals( this.arrayValue );
        }

        @Override
        public int hashCode()
        {
            return arrayValue != null ? arrayValue.hashCode() : 0;
        }
    }
}
