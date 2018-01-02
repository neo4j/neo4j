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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.helpers.Pair;

import static java.lang.String.format;

/**
 * Represents an entity from an input source, for example a .csv file.
 */
public abstract class InputEntity implements SourceTraceability
{
    public static final Object[] NO_PROPERTIES = new Object[0];
    public static final String[] NO_LABELS = new String[0];

    private Object[] properties;
    private final Long firstPropertyId;
    private final String sourceDescription;
    private final long lineNumber;
    private final long position;

    public InputEntity( String sourceDescription, long sourceLineNumber, long sourcePosition,
            Object[] properties, Long firstPropertyId )
    {
        assert properties.length % 2 == 0 : Arrays.toString( properties );

        this.sourceDescription = sourceDescription;
        this.lineNumber = sourceLineNumber;
        this.position = sourcePosition;

        this.properties = properties;
        this.firstPropertyId = firstPropertyId;
    }

    public Object[] properties()
    {
        return properties;
    }

    /**
     * Adds properties to existing properties in this entity. Properties that exist
     * @param keyValuePairs
     */
    public void updateProperties( UpdateBehaviour behaviour, Object... keyValuePairs )
    {
        assert keyValuePairs.length % 2 == 0 : Arrays.toString( keyValuePairs );

        // There were no properties before, just set these and be done
        if ( properties == null || properties.length == 0 )
        {
            setProperties( keyValuePairs );
            return;
        }

        // We need to look at existing properties
        // First make room for any new properties
        int newLength = collectiveNumberOfKeys( properties, keyValuePairs ) * 2;
        properties = newLength == properties.length ? properties : Arrays.copyOf( properties, newLength );
        for ( int i = 0; i < keyValuePairs.length; i++ )
        {
            Object key = keyValuePairs[i++];
            Object value = keyValuePairs[i];
            updateProperty( key, value, behaviour );
        }
    }

    private int collectiveNumberOfKeys( Object[] properties, Object[] otherProperties )
    {
        int collidingKeys = 0;
        for ( int i = 0; i < properties.length; i += 2 )
        {
            Object key = properties[i];
            for ( int j = 0; j < otherProperties.length; j += 2 )
            {
                Object otherKey = otherProperties[j];
                if ( otherKey.equals( key ) )
                {
                    collidingKeys++;
                    break;
                }
            }
        }
        return properties.length/2 + otherProperties.length/2 - collidingKeys;
    }

    private void updateProperty( Object key, Object value, UpdateBehaviour behaviour )
    {
        int free = 0;
        for ( int i = 0; i < properties.length; i++ )
        {
            Object existingKey = properties[i++];
            if ( existingKey == null )
            {
                free = i-1;
                break;
            }
            if ( existingKey.equals( key ) )
            {   // update
                properties[i] = behaviour.merge( properties[i], value );
                return;
            }
        }

        // Add
        properties[free++] = key;
        properties[free] = value;
    }

    public void setProperties( Object... keyValuePairs )
    {
        properties = keyValuePairs;
    }

    public boolean hasFirstPropertyId()
    {
        return firstPropertyId != null;
    }

    public long firstPropertyId()
    {
        return firstPropertyId;
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
    public long lineNumber()
    {
        return lineNumber;
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public String toString()
    {
        Collection<Pair<String,?>> fields = new ArrayList<>();
        toStringFields( fields );

        StringBuilder builder = new StringBuilder( "%s:" );
        Object[] arguments = new Object[fields.size()+1];
        int cursor = 0;
        arguments[cursor++] = getClass().getSimpleName();
        for ( Pair<String, ?> item : fields )
        {
            builder.append( "%n   %s" );
            arguments[cursor++] = item.first() + ": " + item.other();
        }

        return format( builder.append( "%n" ).toString(), arguments );
    }

    protected void toStringFields( Collection<Pair<String, ?>> fields )
    {
        fields.add( Pair.of( "source", sourceDescription + ":" + lineNumber ) );
        if ( hasFirstPropertyId() )
        {
            fields.add( Pair.of( "nextProp", firstPropertyId ) );
        }
        else if ( properties != null && properties.length > 0 )
        {
            fields.add( Pair.of( "properties", Arrays.toString( properties ) ) );
        }
    }
}
