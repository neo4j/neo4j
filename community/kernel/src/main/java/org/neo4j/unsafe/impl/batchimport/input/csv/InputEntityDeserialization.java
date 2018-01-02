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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

/**
 * Temporary data when building an {@link InputEntity}. Reusable for building multiple instances.
 *
 * @see InputEntity
 */
public abstract class InputEntityDeserialization<ENTITY extends InputEntity> implements Deserialization<ENTITY>
{
    protected final SourceTraceability source;

    private Object[] properties = new Object[10*2];
    private int propertiesCursor;

    public InputEntityDeserialization( SourceTraceability source )
    {
        this.source = source;
    }

    public void addProperty( String name, Object value )
    {
        if ( value != null )
        {
            ensurePropertiesArrayCapacity( propertiesCursor+2 );
            properties[propertiesCursor++] = name;
            properties[propertiesCursor++] = value;
        }
        // else it's fine because no value was specified
    }

    protected Object[] properties()
    {
        return propertiesCursor > 0
                ? Arrays.copyOf( properties, propertiesCursor )
                : InputEntity.NO_PROPERTIES;
    }

    @Override
    public void handle( Entry entry, Object value )
    {
        switch ( entry.type() )
        {
        case PROPERTY:
            if ( value != null && value.getClass().isArray() && Array.getLength( value ) == 0 )
            {
                // Extractor will return empty arrays for fields that are empty. We don't need to
                // store empty arrays as properties on entities since queries handle this while reading
                // instead, more efficient overall.
                break;
            }
            addProperty( entry.name(), value );
            break;
        case IGNORE: // value ignored. The call stack shouldn't have come this far, but there's no harm
                     // having this case here, I think.
            break;
        default:
            break;
        }
    }

    private void ensurePropertiesArrayCapacity( int length )
    {
        if ( length > properties.length )
        {
            properties = Arrays.copyOf( properties, length );
        }
    }

    @Override
    public void clear()
    {
        propertiesCursor = 0;
    }
}
