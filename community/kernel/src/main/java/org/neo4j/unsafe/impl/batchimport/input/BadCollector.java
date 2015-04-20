/*
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;

import static java.lang.String.format;
import static java.util.Arrays.copyOf;

import static org.neo4j.helpers.Exceptions.withMessage;

public class BadCollector implements Collector
{
    public static final int BAD_RELATIONSHIPS = 0x1;
    public static final int DUPLICATE_NODES = 0x2;
    public static final int COLLECT_ALL = BAD_RELATIONSHIPS | DUPLICATE_NODES;

    private final PrintStream out;
    private final int tolerance;
    private final int collect;
    private long[] leftOverDuplicateNodeIds = new long[10];
    private int leftOverDuplicateNodeIdsCursor = 0;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private volatile int badEntries;

    public BadCollector( OutputStream out, int tolerance, int collect )
    {
        this.out = new PrintStream( out );
        this.tolerance = tolerance;
        this.collect = collect;
    }

    @Override
    public void collectBadRelationship( InputRelationship relationship, Object specificValue )
    {
        checkTolerance( BAD_RELATIONSHIPS,
                new InputException( format( "%s refering to missing node %s", relationship, specificValue ) ) );
    }

    @Override
    public void collectDuplicateNode( Object id, long actualId, String group, String firstSource, String otherSource )
    {
        checkTolerance( DUPLICATE_NODES, new DuplicateInputIdException( id, group, firstSource, otherSource ) );

        if ( leftOverDuplicateNodeIdsCursor == leftOverDuplicateNodeIds.length )
        {
            leftOverDuplicateNodeIds = Arrays.copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIds.length*2 );
        }
        leftOverDuplicateNodeIds[leftOverDuplicateNodeIdsCursor++] = actualId;
    }

    @Override
    public PrimitiveLongIterator leftOverDuplicateNodesIds()
    {
        return PrimitiveLongCollections.iterator( copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIdsCursor ) );
    }

    @Override
    public void close()
    {
        out.flush();
    }

    @Override
    public int badEntries()
    {
        return badEntries;
    }

    private boolean collects( int bit )
    {
        return (collect & bit) != 0;
    }

    private void checkTolerance( int bit, InputException exception )
    {
        boolean collect = collects( bit );
        if ( collect )
        {
            out.println( exception.getMessage() );
            badEntries++;
        }

        if ( !collect || badEntries > tolerance )
        {
            throw collect
                    ? withMessage( exception, format( "Too many bad entries %d, where last one was: ", badEntries ) +
                            exception.getMessage() )
                    : exception;
        }
    }
}
