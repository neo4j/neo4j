/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.event.PropertyEntry;

class PropertyEntryImpl<T extends PropertyContainer>
    implements PropertyEntry<T>
{
    private final T entity;
    private final String key;
    private final Object value;
    private final Object valueBeforeTransaction;

    private PropertyEntryImpl( T entity, String key, Object value,
            Object valueBeforeTransaction )
    {
        this.entity = entity;
        this.key = key;
        this.value = value;
        this.valueBeforeTransaction = valueBeforeTransaction;
    }
    
    static <T extends PropertyContainer> PropertyEntry<T> assigned( T entity,
            String key, Object value, Object valueBeforeTransaction )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( "Null value" );
        }
        return new PropertyEntryImpl<T>( entity, key, value, valueBeforeTransaction );
    }
    
    static <T extends PropertyContainer> PropertyEntry<T> removed( T entity,
            String key, Object valueBeforeTransaction )
    {
        return new PropertyEntryImpl<T>( entity, key, null, valueBeforeTransaction );
    }
    
    public T entity()
    {
        return this.entity;
    }

    public String key()
    {
        return this.key;
    }

    public Object previouslyCommitedValue()
    {
        return this.valueBeforeTransaction;
    }
    
    public Object value()
    {
        if ( this.value == null )
        {
            throw new IllegalStateException( "PropertyEntry[" + entity + ", "
                    + key + "] has no value, it represents a removed property" );
        }
        return this.value;
    }
    
    @Override
    public String toString()
    {
        return "PropertyEntry[entity:" + entity + ", key:" + key + ", value:" + value +
                ", valueBeforeTx:" + valueBeforeTransaction + "]";
    }
}
