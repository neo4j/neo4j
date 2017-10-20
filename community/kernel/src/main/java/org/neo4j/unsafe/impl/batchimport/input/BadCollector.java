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
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.concurrent.AsyncEvent;
import org.neo4j.concurrent.AsyncEvents;
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
    abstract static class ProblemReporter extends AsyncEvent
    {
        private final int type;

        ProblemReporter( int type )
        {
            this.type = type;
        }

        int type()
        {
            return type;
        }

        abstract String message();

        abstract InputException exception();
    }

    public static final int BAD_RELATIONSHIPS = 0x1;
    public static final int DUPLICATE_NODES = 0x2;
    public static final int EXTRA_COLUMNS = 0x4;
    public static final int COLLECT_ALL = BAD_RELATIONSHIPS | DUPLICATE_NODES | EXTRA_COLUMNS;
    public static final long UNLIMITED_TOLERANCE = -1;

    private final PrintStream out;
    private final long tolerance;
    private final int collect;
    private long[] leftOverDuplicateNodeIds = new long[10];
    private int leftOverDuplicateNodeIdsCursor;
    private final boolean logBadEntries;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private final AtomicLong badEntries = new AtomicLong();
    private final AsyncEvents<ProblemReporter> logger;
    private final Thread eventProcessor;

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
        this.logger = new AsyncEvents<>( this::processEvent, AsyncEvents.Monitor.NONE );
        this.eventProcessor = new Thread( logger );
        this.eventProcessor.start();
    }

    private void processEvent( ProblemReporter report )
    {
        out.println( report.message() );
    }

    @Override
    public void collectBadRelationship( final InputRelationship relationship, final Object specificValue )
    {
        collect( new RelationshipsProblemReporter( relationship, specificValue ) );
    }

    @Override
    public void collectExtraColumns( final String source, final long row, final String value )
    {
        collect( new ExtraColumnsProblemReporter( row, source, value ) );
    }

    @Override
    public void collectDuplicateNode( final Object id, long actualId, final String group,
            final String firstSource, final String otherSource )
    {
        collect( new NodesProblemReporter( id, group, firstSource, otherSource ) );

        // We can do this right in here because as it turns out this is never called by multiple concurrent threads.
        if ( leftOverDuplicateNodeIdsCursor == leftOverDuplicateNodeIds.length )
        {
            leftOverDuplicateNodeIds = Arrays.copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIds.length * 2 );
        }
        leftOverDuplicateNodeIds[leftOverDuplicateNodeIdsCursor++] = actualId;
    }

    @Override
    public PrimitiveLongIterator leftOverDuplicateNodesIds()
    {
        leftOverDuplicateNodeIds = copyOf( leftOverDuplicateNodeIds, leftOverDuplicateNodeIdsCursor );
        sort( leftOverDuplicateNodeIds );
        return PrimitiveLongCollections.iterator( leftOverDuplicateNodeIds );
    }

    private void collect( ProblemReporter report )
    {
        boolean collect = collects( report.type() );
        if ( collect )
        {
            // This type of problem is collected and we're within the max threshold, so it's OK
            long count = badEntries.incrementAndGet();
            if ( tolerance == UNLIMITED_TOLERANCE || count <= tolerance )
            {
                // We're within the threshold
                if ( logBadEntries )
                {
                    // Send this to the logger
                    logger.send( report );
                }
                return; // i.e. don't treat this as an exception
            }
        }

        InputException exception = report.exception();
        throw collect ? withMessage( exception, format( "Too many bad entries %d, where last one was: %s",
                badEntries.longValue(), exception.getMessage() ) ) : exception;
    }

    @Override
    public void close()
    {
        logger.shutdown();
        try
        {
            logger.awaitTermination();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            out.flush();
        }
    }

    @Override
    public long badEntries()
    {
        return badEntries.get();
    }

    private boolean collects( int bit )
    {
        return (collect & bit) != 0;
    }

    private static class RelationshipsProblemReporter extends ProblemReporter
    {
        private String message;
        private final InputRelationship relationship;
        private final Object specificValue;

        RelationshipsProblemReporter( InputRelationship relationship, Object specificValue )
        {
            super( BAD_RELATIONSHIPS );
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

    private static class NodesProblemReporter extends ProblemReporter
    {
        private final Object id;
        private final String group;
        private final String firstSource;
        private final String otherSource;

        NodesProblemReporter( Object id, String group, String firstSource, String otherSource )
        {
            super( DUPLICATE_NODES );
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

    private static class ExtraColumnsProblemReporter extends ProblemReporter
    {
        private String message;
        private final long row;
        private final String source;
        private final String value;

        ExtraColumnsProblemReporter( long row, String source, String value )
        {
            super( EXTRA_COLUMNS );
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
