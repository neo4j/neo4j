/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.event;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.event.PropertyEntry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    
    @Override
    public T entity()
    {
        return this.entity;
    }

    @Override
    public String key()
    {
        return this.key;
    }

    @Override
    public Object value()
    {
        return this.value;
    }

    @Override
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

    @Override
    public String toString()
    {
        return "PropertyEntry[entity=" + entity + ", key=" + key + ", value=" + value + ", valueBeforeTx="
                + valueBeforeTx + "]";
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
