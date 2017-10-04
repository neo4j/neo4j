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
    private final long tolerance;
    private final int collect;
    private long[] leftOverDuplicateNodeIds = new long[10];
    private int leftOverDuplicateNodeIdsCursor;
    private final boolean logBadEntries;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private volatile long badEntries;
    public static final long UNLIMITED_TOLERANCE = -1;

    public BadCollector( OutputStream out, long tolerance, int collect )
    {
        this( out, tolerance, collect, false );
    }

    public BadCollector( OutputStream out, long tolerance, int collect, boolean skipBadEntriesLogging )
    {
        this.out = new PrintStream( out );
        this.tolerance = tolerance;
        this.collect = collect;
        this.logBadEntries = !skipBadEntriesLogging;
    }

    @Override
    public synchronized void collectBadRelationship( final InputRelationship relationship, final Object specificValue )
    {
        checkTolerance( BAD_RELATIONSHIPS, new RelationshipsProblemReporter( relationship, specificValue ) );
    }

    @Override
    public void collectDuplicateNode( final Object id, long actualId, final String group,
            final String firstSource, final String otherSource )
    {
        checkTolerance( DUPLICATE_NODES, new NodesProblemReporter( id, group, firstSource, otherSource ) );

        if ( leftOverDuplicateNodeIdsCursor == leftOverDuplicateNodeIds.length )
        {
            leftOverDuplicateNodeIds = Arrays.copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIds.length * 2 );
        }
        leftOverDuplicateNodeIds[leftOverDuplicateNodeIdsCursor++] = actualId;
    }

    @Override
    public void collectExtraColumns( final String source, final long row, final String value )
    {
        checkTolerance( EXTRA_COLUMNS, new ExtraColumnsProblemReporter( row, source, value ) );
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
    public long badEntries()
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
            if ( logBadEntries )
            {
                out.println( report.message() );
            }
            badEntries++;
        }

        if ( !collect || (tolerance != BadCollector.UNLIMITED_TOLERANCE && badEntries > tolerance) )
        {
            InputException exception = report.exception();
            throw collect ? withMessage( exception, format( "Too many bad entries %d, where last one was: %s",
                    badEntries, exception.getMessage() ) ) : exception;
        }
    }

    private static class RelationshipsProblemReporter implements ProblemReporter
    {
        private String message;
        private final InputRelationship relationship;
        private final Object specificValue;

        RelationshipsProblemReporter( InputRelationship relationship, Object specificValue )
        {
            this.relationship = relationship;
            this.specificValue = specificValue;
        }

        @Override
        public String message()
        {
            return getReportMessage();
        }

        @Override
        public InputException exception()
        {
            return new InputException( getReportMessage() );
        }

        private String getReportMessage()
        {
            if ( message == null )
            {
                message = !isMissingData( relationship )
                        ? format( "%s referring to missing node %s", relationship, specificValue )
                        : format( "%s is missing data", relationship );
            }
            return message;
        }

        private static boolean isMissingData( InputRelationship relationship )
        {
            return relationship.startNode() == null || relationship.endNode() == null || !relationship.hasType();
        }
    }

    private static class NodesProblemReporter implements ProblemReporter
    {
        private final Object id;
        private final String group;
        private final String firstSource;
        private final String otherSource;

        NodesProblemReporter( Object id, String group, String firstSource, String otherSource )
        {
            this.id = id;
            this.group = group;
            this.firstSource = firstSource;
            this.otherSource = otherSource;
        }

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
    }

    private static class ExtraColumnsProblemReporter implements ProblemReporter
    {
        private String message;
        private final long row;
        private final String source;
        private final String value;

        ExtraColumnsProblemReporter( long row, String source, String value )
        {
            this.row = row;
            this.source = source;
            this.value = value;
        }

        @Override
        public String message()
        {
            return getReportMessage();
        }

        @Override
        public InputException exception()
        {
            return new InputException( getReportMessage() );
        }

        private String getReportMessage()
        {
            if ( message == null )
            {
                message = format( "Extra column not present in header on line %d in %s with value %s", row, source, value );
            }
            return message;
        }
    }
}
