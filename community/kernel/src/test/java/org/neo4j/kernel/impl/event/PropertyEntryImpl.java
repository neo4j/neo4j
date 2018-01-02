/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.ObjectUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.ArrayUtil.BOXING_AWARE_ARRAY_EQUALITY;

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
        assertEqualsMaybeNull( entry.value(), value(), entry.entity(), entry.key() );
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
        assertEqualsMaybeNull( entry.previouslyCommitedValue(), previouslyCommitedValue(),
                entry.entity(), entry.key() );
    }

    @Override
    public String toString()
    {
        return "PropertyEntry[entity=" + entity + ", key=" + key + ", value=" + value + ", valueBeforeTx="
                + valueBeforeTx + "]";
    }

    public static <T extends PropertyContainer> void assertEqualsMaybeNull( Object o1, Object o2, T entity, String key )
    {
        String entityDescription = "For " + entity + " and " + key;
        if ( o1 == null || o2 == null )
        {
            assertTrue( entityDescription + ". " + ObjectUtil.toString( o1 ) + " != " + ObjectUtil.toString( o2 ), o1 == o2 );
        }
        else
        {
            assertEquals( o1.getClass().isArray(), o2.getClass().isArray() );
            if ( o1.getClass().isArray() )
            {
                assertTrue( entityDescription + " (" + o1.getClass().getComponentType().getSimpleName() + ") " +
                        ObjectUtil.toString( o1 ) + " not equal to " + ObjectUtil.toString( o2 ),
                        ArrayUtil.equals( o1, o2, BOXING_AWARE_ARRAY_EQUALITY ) );
            }
            else
            {
                assertEquals( entityDescription, o1, o2 );
            }
        }
    }
}
