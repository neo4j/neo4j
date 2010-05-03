package org.neo4j.kernel.impl.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.event.PropertyEntry;

class PropertyEntryImpl<T extends PropertyContainer> implements PropertyEntry<T>
{
    private final T entity;
    private final String key;
    private final Object value;
    private final Object valueBeforeTx;

    PropertyEntryImpl( T entity, String key, Object value, Object valueBeforeTx )
    {
        this.entity = entity;
        this.key = key;
        this.value = value;
        this.valueBeforeTx = valueBeforeTx;
    }
    
    public T entity()
    {
        return this.entity;
    }

    public String key()
    {
        return this.key;
    }

    public Object value()
    {
        return this.value;
    }

    public Object previouslyCommitedValue()
    {
        return this.valueBeforeTx;
    }
    
    void compareToAssigned( PropertyEntry<T> entry )
    {
        basicCompareTo( entry );
        assertEqualsMaybeNull( entry.value(), value() );
    }

    void compareToRemoved( PropertyEntry<T> entry )
    {
        basicCompareTo( entry );
        try
        {
            entry.value();
            fail( "Should throw IllegalStateException" );
        }
        catch ( IllegalStateException e )
        {
            // OK
        }
        assertNull( value() );
    }
    
    void basicCompareTo( PropertyEntry<T> entry )
    {
        assertEquals( entry.entity(), entity() );
        assertEquals( entry.key(), key() );
        assertEqualsMaybeNull( entry.previouslyCommitedValue(), previouslyCommitedValue() );
    }

    public static void assertEqualsMaybeNull( Object o1, Object o2 )
    {
        if ( o1 == null )
        {
            assertTrue( o1 == o2 );
        }
        else
        {
            assertEquals( o1, o2 );
        }
    }
}
