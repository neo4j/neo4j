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
package org.neo4j.tooling;

import java.util.Random;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.lang.Math.abs;

/**
 * Generates random data based on a {@link Header} and some statistics, such as label/relationship type counts.
 */
public class RandomDataIterator<T> extends PrefetchingIterator<T> implements InputIterator<T>
{
    private final Header header;
    private final long limit;
    private final Random random;
    private final Deserialization<T> deserialization;
    private final long nodeCount;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;

    private long cursor;
    private long position;

    public RandomDataIterator( Header header, long limit, Random random,
            Deserialization<T> deserialization, long nodeCount,
            int labelCount, int relationshipTypeCount )
    {
        this.header = header;
        this.limit = limit;
        this.random = random;
        this.deserialization = deserialization;
        this.nodeCount = nodeCount;
        this.labels = new Distribution<>( tokens( "Label", labelCount ) );
        this.relationshipTypes = new Distribution<>( tokens( "TYPE", relationshipTypeCount ) );
    }

    private String[] tokens( String prefix, int count )
    {
        String[] result = new String[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = prefix + count;
        }
        return result;
    }

    @Override
    protected T fetchNextOrNull()
    {
        if ( cursor < limit )
        {
            try
            {
                return generateDataLine();
            }
            finally
            {
                cursor++;
            }
        }
        return null;
    }

    private T generateDataLine()
    {
        for ( Entry entry : header.entries() )
        {
            switch ( entry.type() )
            {
            case ID:
                deserialization.handle( entry, idValue( entry, cursor ) );
                break;
            case PROPERTY:
                deserialization.handle( entry, randomProperty( entry, random ) );
                break;
            case LABEL:
                deserialization.handle( entry, randomLabels( random ) );
                break;
            case START_ID: case END_ID:
                deserialization.handle( entry, idValue( entry, abs( random.nextLong() ) % nodeCount ) );
                break;
            case TYPE:
                deserialization.handle( entry, randomRelationshipType( random ) );
                break;
            default:
                throw new IllegalArgumentException( entry.toString() );
            }
        }
        try
        {
            return deserialization.materialize();
        }
        finally
        {
            deserialization.clear();
        }
    }

    private Object idValue( Entry entry, long id )
    {
        switch ( entry.extractor().toString() )
        {
        case "String": return "" + id;
        case "long": return id;
        default: throw new IllegalArgumentException( entry.toString() );
        }
    }

    private String randomRelationshipType( Random random )
    {
        position += 6;
        return relationshipTypes.random( random );
    }

    private Object randomProperty( Entry entry, Random random )
    {
        // TODO crude way of determining value type
        String type = entry.extractor().toString();
        if ( type.equals( "String" ) )
        {
            return randomString( random );
        }
        else if ( type.equals( "long" ) )
        {
            position += 8; // sort of
            return random.nextInt( Integer.MAX_VALUE );
        }
        else if ( type.equals( "int" ) )
        {
            position += 4; // sort of
            return random.nextInt( 20 );
        }
        else
        {
            throw new IllegalArgumentException( "" + entry );
        }
    }

    private String[] randomLabels( Random random )
    {
        int length = random.nextInt( 3 );
        if ( length == 0 )
        {
            return InputEntity.NO_LABELS;
        }

        String[] result = new String[length];
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = labels.random( random );
        }
        position += length * 6;
        return result;
    }

    private String randomString( Random random )
    {
        char[] chars = new char[random.nextInt( 10 )+5];
        for ( int i = 0; i < chars.length; i++ )
        {
            chars[i] = (char) ('a' + random.nextInt( 20 ));
        }
        position += chars.length;
        return String.valueOf( chars );
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    @Override
    public long position()
    {
        return position;
    }
}