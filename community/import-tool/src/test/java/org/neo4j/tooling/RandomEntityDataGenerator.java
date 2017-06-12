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
import org.neo4j.unsafe.impl.batchimport.GeneratingInputIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.RandomsStates;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.lang.Math.abs;

import static org.neo4j.unsafe.impl.batchimport.input.CachingInputEntityVisitor.NO_LABELS;

/**
 * Data generator as {@link InputIterator}, parallelizable
 */
public class RandomEntityDataGenerator extends GeneratingInputIterator<Randoms>
{
    private final Header header;
    private final int batchSize;
    private final long nodeCount;
    private final Distribution<String> labels;
    private final Distribution<String> relationshipTypes;
    private final long count;

    public RandomEntityDataGenerator( long nodeCount, long count, int batchSize, long seed, Header header,
           Distribution<String> labels, Distribution<String> relationshipTypes )
    {
        super( new RandomsStates( seed, count, batchSize ) );
        this.nodeCount = nodeCount;
        this.count = count;
        this.batchSize = batchSize;
        this.header = header;
        this.labels = labels;
        this.relationshipTypes = relationshipTypes;
    }

    @Override
    protected boolean generateNext( Randoms randoms, long batch, int itemInBatch, InputEntityVisitor visitor )
    {
        long id = batch * batchSize + itemInBatch;
        if ( id >= count )
        {
            return false;
        }

        for ( Entry entry : header.entries() )
        {
            switch ( entry.type() )
            {
            case ID:
                visitor.id( idValue( entry, id ), entry.group() );
                break;
            case PROPERTY:
                visitor.property( entry.name(), randomProperty( entry, randoms ) );
                break;
            case LABEL:
                visitor.labels( randomLabels( randoms.random() ) );
                break;
            case START_ID:
                visitor.startId( idValue( entry, abs( randoms.random().nextLong() ) % nodeCount ), entry.group() );
                break;
            case END_ID:
                visitor.endId( idValue( entry, abs( randoms.random().nextLong() ) % nodeCount ), entry.group() );
                break;
            case TYPE:
                visitor.type( randomRelationshipType( randoms.random() ) );
                break;
            default:
                throw new IllegalArgumentException( entry.toString() );
            }
        }
        return true;
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
        return relationshipTypes.random( random );
    }

    private Object randomProperty( Entry entry, Randoms random )
    {
        // TODO crude way of determining value type
        String type = entry.extractor().toString();
        if ( type.equals( "String" ) )
        {
            return random.string( 5, 20, Randoms.CSA_LETTERS_AND_DIGITS );
        }
        else if ( type.equals( "long" ) )
        {
            return random.nextInt( Integer.MAX_VALUE );
        }
        else if ( type.equals( "int" ) )
        {
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
            return NO_LABELS;
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
        return result;
    }
}
