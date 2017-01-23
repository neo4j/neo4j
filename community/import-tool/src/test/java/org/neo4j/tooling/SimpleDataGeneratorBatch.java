/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.test.Randoms;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.lang.Math.abs;

class SimpleDataGeneratorBatch<T>
{
    private final Header header;
    private final Random random;
    private final Randoms randoms;
    private final long nodeCount;
    private final long start;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;
    private final Deserialization<T> deserialization;
    private final T[] target;

    private long cursor;
    private long position;

    SimpleDataGeneratorBatch(
            Header header, long start, long randomSeed, long nodeCount,
            Distribution<String> labels, Distribution<String> relationshipTypes,
            Deserialization<T> deserialization, T[] target )
    {
        this.header = header;
        this.start = start;
        this.nodeCount = nodeCount;
        this.labels = labels;
        this.relationshipTypes = relationshipTypes;
        this.target = target;
        this.random = new Random( randomSeed );
        this.randoms = new Randoms( random, Randoms.DEFAULT );
        this.deserialization = deserialization;

        deserialization.initialize();
    }

    T[] get()
    {
        for ( int i = 0; i < target.length; i++ )
        {
            target[i] = next();
        }
        return target;
    }

    private T next()
    {
        for ( Entry entry : header.entries() )
        {
            switch ( entry.type() )
            {
            case ID:
                deserialization.handle( entry, idValue( entry, start + cursor ) );
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
            cursor++;
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
            return randoms.string( 5, 20, Randoms.CSA_LETTERS_AND_DIGITS );
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
        for ( int i = 0; i < result.length; )
        {
            String candidate = labels.random( random );
            if ( !ArrayUtil.contains( result, i, candidate ) )
            {
                result[i++] = candidate;
            }
        }
        position += length * 6;
        return result;
    }
}
