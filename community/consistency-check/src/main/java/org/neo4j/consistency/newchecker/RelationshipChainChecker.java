/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency.newchecker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.newchecker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.time.Stopwatch;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.NEXT;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.PREV;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_FIRST_IN_CHAIN;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_HAS_MULTIPLE_RELATIONSHIPS;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_IN_USE;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_PREV_OR_NEXT;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_REFERENCE;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_RELATIONSHIP_ID;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_SOURCE_OR_TARGET;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SOURCE;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.TARGET;
import static org.neo4j.consistency.checking.cache.CacheSlots.longOf;
import static org.neo4j.consistency.newchecker.RelationshipLink.SOURCE_NEXT;
import static org.neo4j.consistency.newchecker.RelationshipLink.SOURCE_PREV;
import static org.neo4j.consistency.newchecker.RelationshipLink.TARGET_NEXT;
import static org.neo4j.consistency.newchecker.RelationshipLink.TARGET_PREV;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * Checks relationship chains, i.e. their internal pointers between relationship records.
 */
class RelationshipChainChecker implements Checker
{
    private static final int REPORT_PAGE_ID_THRESHOLD = 1000;
    private final int ioReadAheadSize;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final int numberOfChainCheckers;
    private final CacheAccess cacheAccess;
    private final RecordLoading recordLoader;
    private final ProgressListener progress;

    RelationshipChainChecker( CheckerContext context )
    {
        this.context = context;
        this.reporter = context.reporter;
        // Because the last thread will be the one reading the relationships store and enqueuing to the checkers
        this.numberOfChainCheckers = max( 1, context.execution.getNumberOfThreads() - 2 );
        this.cacheAccess = context.cacheAccess;
        this.recordLoader = context.recordLoader;
        this.progress = context.progressReporter( this, "Relationship chains", context.neoStores.getRelationshipStore().getHighId() * 2 );
        this.ioReadAheadSize = max( REPORT_PAGE_ID_THRESHOLD * 2, min( 100_000, toIntExact( context.pageCache.maxCachedPages() / 4 ) ) );
    }

    @Override
    public void check( LongRange nodeIdRange, boolean firstRange, boolean lastRange ) throws Exception
    {
        // Forward scan (cache prev pointers)
        checkDirection( nodeIdRange, ScanDirection.FORWARD );

        // Backward scan (cache next pointers)
        context.paddedDebug( "%s moving over to backwards relationship chain checking", getClass().getSimpleName() );
        checkDirection( nodeIdRange, ScanDirection.BACKWARD );
    }

    private void checkDirection( LongRange nodeIdRange, ScanDirection direction ) throws Exception
    {
        RelationshipStore relationshipStore = context.neoStores.getRelationshipStore();
        long highId = relationshipStore.getHighId();
        AtomicBoolean end = new AtomicBoolean();
        int numberOfThreads = numberOfChainCheckers + 2;
        ThrowingRunnable[] workers = new ThrowingRunnable[numberOfThreads];
        ProgressListener localProgress = progress.threadLocalReporter();
        ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues = new ArrayBlockingQueue[numberOfChainCheckers];
        BatchedRelationshipRecords[] threadBatches = new BatchedRelationshipRecords[numberOfChainCheckers];
        AtomicLong currentWorkingPage = new AtomicLong( 0 );
        for ( int i = 0; i < numberOfChainCheckers; i++ )
        {
            threadQueues[i] = new ArrayBlockingQueue<>( 20 );
            threadBatches[i] = new BatchedRelationshipRecords();
            workers[i] = relationshipVsRelationshipChecker( nodeIdRange, direction, relationshipStore, threadQueues[i], end, i );
        }

        // Record reader
        workers[workers.length - 2] = () ->
        {
            RelationshipRecord relationship = relationshipStore.newRecord();
            try ( PageCursor cursor = relationshipStore.openPageCursorForReading( 0 ) )
            {
                int recordsPerPage = relationshipStore.getRecordsPerPage();
                long id = direction.startingId( highId );
                while ( id >= 0 && id < highId && !context.isCancelled() )
                {
                    for ( int i = 0; i < recordsPerPage && id >= 0 && id < highId; i++, id = direction.nextId( id ) )
                    {
                        relationshipStore.getRecordByCursor( id, relationship, RecordLoad.CHECK, cursor );
                        localProgress.add( 1 );
                        if ( relationship.inUse() )
                        {
                            queueRelationshipCheck( threadQueues, threadBatches, relationship );
                        }
                    }

                    if ( cursor.getCurrentPageId() % REPORT_PAGE_ID_THRESHOLD == 0 )
                    {
                        currentWorkingPage.set( cursor.getCurrentPageId() );
                    }
                }
                processLastRelationshipChecks( threadQueues, threadBatches, end );
                localProgress.done();
            }
        };

        // I/O worker that paves the way for the record reader so that it won't have to spend time page faulting
        workers[workers.length - 1] = () ->
        {
            StorePagePrefetcher prefetcher =
                    new StorePagePrefetcher( relationshipStore, ioReadAheadSize, context::isCancelled, StorePagePrefetcher.NO_MONITOR );
            prefetcher.prefetch( currentWorkingPage::get, direction == ScanDirection.FORWARD );
        };

        Stopwatch stopwatch = Stopwatch.start();
        cacheAccess.clearCache();
        context.execution.runAll( getClass().getSimpleName() + "-" + direction.name(), workers );
        detectSingleRelationshipChainInconsistencies( nodeIdRange );
        context.paddedDebug( "%s %s took %s", this, direction, duration( stopwatch.elapsed( TimeUnit.MILLISECONDS ) ) );
    }

    @Override
    public boolean shouldBeChecked( ConsistencyFlags flags )
    {
        return flags.isCheckGraph();
    }

    private void detectSingleRelationshipChainInconsistencies( LongRange nodeIdRange )
    {
        CacheAccess.Client client = cacheAccess.client();
        for ( long nodeId = nodeIdRange.from(); nodeId < nodeIdRange.to(); nodeId++ )
        {
            boolean inUse = client.getBooleanFromCache( nodeId, SLOT_IN_USE );
            boolean hasMultipleRelationships = client.getBooleanFromCache( nodeId, SLOT_HAS_MULTIPLE_RELATIONSHIPS );
            if ( inUse && !hasMultipleRelationships )
            {
                long reference = client.getFromCache( nodeId, SLOT_REFERENCE );
                long relationshipId = client.getFromCache( nodeId, SLOT_RELATIONSHIP_ID );
                long sourceOrTarget = client.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET );
                long prevOrNext = client.getFromCache( nodeId, SLOT_PREV_OR_NEXT );
                boolean isFirstInChain = client.getBooleanFromCache( nodeId, SLOT_FIRST_IN_CHAIN );

                boolean consistent;
                if ( prevOrNext == PREV )
                {
                    // 1 is the expected degree of a prev reference for a relationship that is first in chain of length 1
                    consistent = reference == 1 && isFirstInChain;
                }
                else
                {
                    consistent = NULL_REFERENCE.is( reference );
                }

                if ( !consistent )
                {
                    RelationshipStore relationshipStore = context.neoStores.getRelationshipStore();
                    RelationshipRecord relationship = relationshipStore.getRecord( relationshipId, relationshipStore.newRecord(), RecordLoad.FORCE );
                    RelationshipRecord referenceRelationship = relationshipStore.getRecord( reference, relationshipStore.newRecord(), RecordLoad.FORCE );
                    linkOf( sourceOrTarget == SOURCE, prevOrNext == PREV ).reportDoesNotReferenceBack( reporter, relationship, referenceRelationship );
                }
            }
        }
    }

    private static RelationshipLink linkOf( boolean source, boolean prev )
    {
        if ( source )
        {
            return prev ? SOURCE_PREV : SOURCE_NEXT;
        }
        return prev ? TARGET_PREV : TARGET_NEXT;
    }

    private ThrowingRunnable relationshipVsRelationshipChecker( LongRange nodeIdRange, ScanDirection direction, RelationshipStore store,
            ArrayBlockingQueue<BatchedRelationshipRecords> queue, AtomicBoolean end, int threadId )
    {
        final RelationshipRecord relationship = store.newRecord();
        final RelationshipRecord otherRelationship = store.newRecord();
        final CacheAccess.Client client = cacheAccess.client();
        final RelationshipLink sourceCachePointer = direction.sourceLink;
        final RelationshipLink targetCachePointer = direction.targetLink;
        final long prevOrNext = direction.cacheSlot;
        return () ->
        {
            try ( PageCursor otherRelationshipCursor = store.openPageCursorForReading( 0 ) )
            {
                while ( (!end.get() || !queue.isEmpty()) && !context.isCancelled() )
                {
                    BatchedRelationshipRecords batch = queue.poll( 100, TimeUnit.MILLISECONDS );
                    if ( batch != null )
                    {
                        while ( batch.fillNext( relationship ) && !context.isCancelled() )
                        {
                            long firstNode = relationship.getFirstNode();
                            long secondNode = relationship.getSecondNode();
                            // Intentionally not checking nodes outside highId of node store because RelationshipChecker will spot this inconsistency
                            boolean processStartNode =
                                    Math.abs( firstNode % numberOfChainCheckers ) == threadId && nodeIdRange.isWithinRangeExclusiveTo( firstNode );
                            boolean processEndNode =
                                    Math.abs( secondNode % numberOfChainCheckers ) == threadId && nodeIdRange.isWithinRangeExclusiveTo( secondNode );
                            if ( processStartNode )
                            {
                                checkRelationshipLink( direction, SOURCE_PREV, relationship, client, otherRelationship, otherRelationshipCursor, store );
                                checkRelationshipLink( direction, SOURCE_NEXT, relationship, client, otherRelationship, otherRelationshipCursor, store );
                            }
                            if ( processEndNode )
                            {
                                checkRelationshipLink( direction, TARGET_PREV, relationship, client, otherRelationship, otherRelationshipCursor, store );
                                checkRelationshipLink( direction, TARGET_NEXT, relationship, client, otherRelationship, otherRelationshipCursor, store );
                            }
                            if ( processStartNode )
                            {
                                boolean wasInUse = client.getBooleanFromCache( firstNode, SLOT_IN_USE );
                                long link = sourceCachePointer.link( relationship );
                                if ( link < NULL_REFERENCE.longValue() )
                                {
                                    sourceCachePointer.reportDoesNotReferenceBack( reporter, relationship, otherRelationship );
                                }
                                else
                                {
                                    client.putToCache( firstNode, relationship.getId(), link, SOURCE, prevOrNext, 1,
                                            longOf( wasInUse ), longOf( relationship.isFirstInFirstChain() ) );
                                }

                            }
                            if ( processEndNode )
                            {
                                boolean wasInUse = client.getBooleanFromCache( secondNode, SLOT_IN_USE );

                                long link = targetCachePointer.link( relationship );
                                if ( link < NULL_REFERENCE.longValue() )
                                {
                                    targetCachePointer.reportDoesNotReferenceBack( reporter, relationship, otherRelationship );
                                }
                                else
                                {
                                    client.putToCache( secondNode, relationship.getId(), link, TARGET, prevOrNext, 1,
                                            longOf( wasInUse ), longOf( relationship.isFirstInSecondChain() ) );
                                }

                            }
                        }
                    }
                }
            }
        };
    }

    private void checkRelationshipLink( ScanDirection direction, RelationshipLink link, RelationshipRecord relationshipCursor,
            CacheAccess.Client client, RelationshipRecord otherRelationship, PageCursor otherRelationshipCursor, RelationshipStore store )
    {
        long relationshipId = relationshipCursor.getId();
        long nodeId = link.node( relationshipCursor );
        long linkId = link.link( relationshipCursor );
        long fromCache = client.getFromCache( nodeId, SLOT_RELATIONSHIP_ID );
        boolean cachedLinkInUse = client.getBooleanFromCache( nodeId, SLOT_IN_USE );
        if ( !link.endOfChain( relationshipCursor ) && cachedLinkInUse )
        {
            if ( fromCache != linkId )
            {
                // We can't use the cache since it doesn't contain the relationship right before us in this chain
                if ( direction.exclude( relationshipId, linkId ) )
                {
                    return;
                }
                else if ( !NULL_REFERENCE.is( fromCache ) )
                {
                    // Load it from store
                    store.getRecordByCursor( linkId, otherRelationship, RecordLoad.FORCE, otherRelationshipCursor );
                }
                else
                {
                    otherRelationship.clear();
                    link.reportDoesNotReferenceBack( reporter, recordLoader.relationship( relationshipCursor.getId() ), otherRelationship );
                }
            }
            else
            {
                // OK good we can use the cached values representing a relationship right before us in this chain
                otherRelationship.clear();
                otherRelationship.setId( linkId );
                long other = client.getFromCache( nodeId, SLOT_REFERENCE );
                NodeLink nodeLink = client.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE ? NodeLink.SOURCE : NodeLink.TARGET;
                nodeLink.setNode( otherRelationship, nodeId );
                link.setOther( otherRelationship, nodeLink, other );
                otherRelationship.setInUse( client.getBooleanFromCache( nodeId, SLOT_IN_USE ) );
                otherRelationship.setCreated();
            }
            checkRelationshipLink( direction, link, otherRelationship, relationshipId, nodeId, linkId );
        }
    }

    private void checkRelationshipLink( ScanDirection direction, RelationshipLink thing, RelationshipRecord otherRelationship, long relationshipId, long nodeId,
            long linkId )
    {
        // Perform the checks
        NodeLink nodeLink = NodeLink.select( otherRelationship, nodeId );
        if ( nodeLink == null )
        {
            thing.reportOtherNode( reporter, recordLoader.relationship( relationshipId ), recordLoader.relationship( linkId ) );
        }
        else
        {
            if ( thing.other( otherRelationship, nodeLink ) != relationshipId )
            {
                // Read the relationship from store and do the check on that actual record instead, should happen rarely anyway
                if ( otherRelationship.isCreated() )
                {
                    recordLoader.relationship( otherRelationship, otherRelationship.getId() );
                    // Call this method one more time, now with !created
                    checkRelationshipLink( direction, thing, otherRelationship, relationshipId, nodeId, linkId );
                    return;
                }

                thing.reportDoesNotReferenceBack( reporter, recordLoader.relationship( relationshipId ), recordLoader.relationship( linkId ) );
            }
            else
            {
                if ( !direction.exclude( relationshipId, linkId ) && !otherRelationship.inUse() )
                {
                    thing.reportNotUsedRelationshipReferencedInChain( reporter, recordLoader.relationship( relationshipId ),
                            recordLoader.relationship( linkId ) );
                }
            }
        }
    }

    private void queueRelationshipCheck( ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues, BatchedRelationshipRecords[] threadBatches,
            RelationshipRecord relationshipCursor ) throws InterruptedException
    {
        int sourceThread = (int) Math.abs( relationshipCursor.getFirstNode() % numberOfChainCheckers );
        queueRelationshipCheck( threadQueues, threadBatches, relationshipCursor, sourceThread );
        int targetThread = (int) Math.abs( relationshipCursor.getSecondNode() % numberOfChainCheckers );
        if ( targetThread != sourceThread )
        {
            queueRelationshipCheck( threadQueues, threadBatches, relationshipCursor, targetThread );
        }
    }

    private void queueRelationshipCheck( ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues, BatchedRelationshipRecords[] threadBatches,
            RelationshipRecord relationshipCursor, int thread ) throws InterruptedException
    {
        if ( !threadBatches[thread].hasMoreSpace() )
        {
            threadQueues[thread].put( threadBatches[thread] );
            threadBatches[thread] = new BatchedRelationshipRecords();
        }
        threadBatches[thread].add( relationshipCursor );
    }

    private void processLastRelationshipChecks( ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues, BatchedRelationshipRecords[] threadBatches,
            AtomicBoolean end ) throws Exception
    {
        for ( int i = 0; i < threadBatches.length; i++ )
        {
            if ( threadBatches[i].numberOfRelationships() > 0 )
            {
                threadQueues[i].put( threadBatches[i] );
            }
        }
        end.set( true );
    }

    @Override
    public String toString()
    {
        return String.format( "%s[highId:%d]",getClass().getSimpleName(), context.neoStores.getRelationshipStore().getHighId() );
    }

    private enum ScanDirection
    {
        FORWARD( SOURCE_PREV, TARGET_PREV, PREV )
        {
            @Override
            boolean exclude( long id, long reference )
            {
                return !NULL_REFERENCE.is( reference ) && reference > id;
            }

            @Override
            long nextId( long id )
            {
                return id + 1;
            }

            @Override
            long startingId( long highId )
            {
                return 0;
            }
        },
        BACKWARD( SOURCE_NEXT, TARGET_NEXT, NEXT )
        {
            @Override
            boolean exclude( long id, long reference )
            {
                return !NULL_REFERENCE.is( reference ) && reference < id;
            }

            @Override
            long nextId( long id )
            {
                return id - 1;
            }

            @Override
            long startingId( long highId )
            {
                return highId - 1;
            }
        };

        final RelationshipLink sourceLink;
        final RelationshipLink targetLink;
        final long cacheSlot;

        ScanDirection( RelationshipLink sourceLink, RelationshipLink targetLink, long cacheSlot )
        {
            this.sourceLink = sourceLink;
            this.targetLink = targetLink;
            this.cacheSlot = cacheSlot;
        }

        abstract boolean exclude( long id, long reference );

        abstract long nextId( long id );

        abstract long startingId( long highId );
    }
}
