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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;

import static java.lang.String.format;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.sort;

import static org.neo4j.helpers.Exceptions.withMessage;

public class BadCollector implements Collector
{
    /**
     * Introduced to avoid creating an exception for every reported bad thing, since it can be
     * quite the performance hogger for scenarios where there are many many bad things to collect.
     */
    private interface ProblemReporter
    {
        String message();

        InputException exception();
    }

    public static final int BAD_RELATIONSHIPS = 0x1;
    public static final int DUPLICATE_NODES = 0x2;
    public static final int EXTRA_COLUMNS = 0x4;
    public static final int COLLECT_ALL = BAD_RELATIONSHIPS | DUPLICATE_NODES | EXTRA_COLUMNS;

    private final PrintStream out;
    private final int tolerance;
    private final int collect;
    private long[] leftOverDuplicateNodeIds = new long[10];
    private int leftOverDuplicateNodeIdsCursor;

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
    public void collectBadRelationship( final InputRelationship relationship, final Object specificValue )
    {
        checkTolerance( BAD_RELATIONSHIPS, new ProblemReporter()
        {
            private final String message = format( "%s refering to missing node %s", relationship, specificValue );

            @Override
            public String message()
            {
                return message;
            }

            @Override
            public InputException exception()
            {
                return new InputException( message );
            }
        } );
    }

    @Override
    public void collectDuplicateNode( final Object id, long actualId, final String group,
            final String firstSource, final String otherSource )
    {
        checkTolerance( DUPLICATE_NODES, new ProblemReporter()
        {
            @Override
            public String message()
            {
                return DuplicateInputIdException.message( id, group, firstSource, otherSource );
            }

            @Override
            public InputException exception()
            {
                return new DuplicateInputIdException( id, group, firstSource, otherSource );
            }
        } );

        if ( leftOverDuplicateNodeIdsCursor == leftOverDuplicateNodeIds.length )
        {
            leftOverDuplicateNodeIds = Arrays.copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIds.length*2 );
        }
        leftOverDuplicateNodeIds[leftOverDuplicateNodeIdsCursor++] = actualId;
    }

    @Override
    public void collectExtraColumns( final String source, final long row, final String value )
    {
        checkTolerance( EXTRA_COLUMNS, new ProblemReporter()
        {
            private final String message = format( "Extra column not present in header on line %d in %s with value %s",
                    row, source, value );

            @Override
            public String message()
            {
                return message;
            }

            @Override
            public InputException exception()
            {
                return new InputException( message );
            }
        } );
    }

    @Override
    public PrimitiveLongIterator leftOverDuplicateNodesIds()
    {
        leftOverDuplicateNodeIds = copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIdsCursor );
        sort( leftOverDuplicateNodeIds );
        return PrimitiveLongCollections.iterator( leftOverDuplicateNodeIds );
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

    private void checkTolerance( int bit, ProblemReporter report )
    {
        boolean collect = collects( bit );
        if ( collect )
        {
            out.println( report.message() );
            badEntries++;
        }

        if ( !collect || badEntries > tolerance )
        {
            InputException exception = report.exception();
            throw collect
                    ? withMessage( exception, format( "Too many bad entries %d, where last one was: %s", badEntries,
                            exception.getMessage() ) )
                    : exception;
        }
    }
}
