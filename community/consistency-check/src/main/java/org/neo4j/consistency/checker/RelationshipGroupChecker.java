/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checker;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordRelationshipScanCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * Checks relationship groups vs the relationships and node refer to.
 */
class RelationshipGroupChecker implements Checker
{
    private static final String RELATIONSHIP_GROUPS_CHECKER_TAG = "relationshipGroupsChecker";
    private final NeoStores neoStores;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final ProgressListener progress;

    RelationshipGroupChecker( CheckerContext context )
    {
        this.neoStores = context.neoStores;
        this.reporter = context.reporter;
        this.context = context;
        this.progress = context.progressReporter( this, "Relationship groups", neoStores.getRelationshipGroupStore().getHighId() );
    }

    @Override
    public void check( LongRange nodeIdRange, boolean firstRange, boolean lastRange ) throws Exception
    {
        ParallelExecution execution = context.execution;
        execution.run( getClass().getSimpleName(), execution.partition( neoStores.getRelationshipGroupStore(),
                ( from, to, last ) -> () -> check( nodeIdRange, firstRange, from, to, context.pageCacheTracer ) ) );
    }

    @Override
    public boolean shouldBeChecked( ConsistencyFlags flags )
    {
        return flags.isCheckGraph();
    }

    private void check( LongRange nodeIdRange, boolean firstRound, long fromGroupId, long toGroupId, PageCacheTracer pageCacheTracer )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( RELATIONSHIP_GROUPS_CHECKER_TAG );
              RecordReader<RelationshipGroupRecord> groupReader = new RecordReader<>( neoStores.getRelationshipGroupStore(), cursorTracer );
              RecordReader<RelationshipGroupRecord> comparativeReader = new RecordReader<>( neoStores.getRelationshipGroupStore(), cursorTracer );
              RecordStorageReader reader = new RecordStorageReader( neoStores );
              RecordRelationshipScanCursor relationshipCursor = reader.allocateRelationshipScanCursor( cursorTracer ) )
        {
            ProgressListener localProgress = progress.threadLocalReporter();
            CacheAccess.Client client = context.cacheAccess.client();
            for ( long id = fromGroupId; id < toGroupId && !context.isCancelled(); id++ )
            {
                localProgress.add( 1 );
                RelationshipGroupRecord record = groupReader.read( id );
                if ( !record.inUse() )
                {
                    continue;
                }

                long owningNode = record.getOwningNode();
                if ( nodeIdRange.isWithinRangeExclusiveTo( owningNode ) )
                {
                    long cachedOwnerNextRel = client.getFromCache( owningNode, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID );
                    boolean nodeIsInUse = client.getBooleanFromCache( owningNode, CacheSlots.NodeLink.SLOT_IN_USE );
                    if ( !nodeIsInUse )
                    {
                        reporter.forRelationshipGroup( record ).ownerNotInUse();
                    }
                    else if ( cachedOwnerNextRel == id )
                    {
                        // The old checker only verified that the relationship group that node.nextGroup pointed to had this node as its owner
                        client.putToCacheSingle( owningNode, CacheSlots.NodeLink.SLOT_CHECK_MARK, 0 );
                    }
                }

                if ( firstRound )
                {
                    if ( owningNode < 0 )
                    {
                        reporter.forRelationshipGroup( record ).illegalOwner();
                    }
                    checkValidToken( record, record.getType(), context.tokenHolders.relationshipTypeTokens(), neoStores.getRelationshipTypeTokenStore(),
                            ( group, token ) -> reporter.forRelationshipGroup( group ).illegalRelationshipType(),
                            ( group, token ) -> reporter.forRelationshipGroup( group ).relationshipTypeNotInUse( token ), cursorTracer );

                    if ( !NULL_REFERENCE.is( record.getNext() ) )
                    {
                        RelationshipGroupRecord comparativeRecord = comparativeReader.read( record.getNext() );
                        if ( !comparativeRecord.inUse() )
                        {
                            reporter.forRelationshipGroup( record ).nextGroupNotInUse();
                        }
                        else
                        {
                            if ( record.getType() >= comparativeRecord.getType() )
                            {
                                reporter.forRelationshipGroup( record ).invalidTypeSortOrder();
                            }
                            if ( owningNode != comparativeRecord.getOwningNode() )
                            {
                                reporter.forRelationshipGroup( record ).nextHasOtherOwner( comparativeRecord );
                            }
                        }
                    }

                    checkRelationshipGroupRelationshipLink( relationshipCursor, record, record.getFirstOut(), RelationshipGroupLink.OUT,
                            group -> reporter.forRelationshipGroup( group ).firstOutgoingRelationshipNotInUse(),
                            group -> reporter.forRelationshipGroup( group ).firstOutgoingRelationshipNotFirstInChain(),
                            group -> reporter.forRelationshipGroup( group ).firstOutgoingRelationshipOfOtherType(),
                            ( group, rel ) -> reporter.forRelationshipGroup( group ).firstOutgoingRelationshipDoesNotShareNodeWithGroup( rel ),
                            cursorTracer );
                    checkRelationshipGroupRelationshipLink( relationshipCursor, record, record.getFirstIn(), RelationshipGroupLink.IN,
                            group -> reporter.forRelationshipGroup( group ).firstIncomingRelationshipNotInUse(),
                            group -> reporter.forRelationshipGroup( group ).firstIncomingRelationshipNotFirstInChain(),
                            group -> reporter.forRelationshipGroup( group ).firstIncomingRelationshipOfOtherType(),
                            ( group, rel ) -> reporter.forRelationshipGroup( group ).firstIncomingRelationshipDoesNotShareNodeWithGroup( rel ),
                            cursorTracer );
                    checkRelationshipGroupRelationshipLink( relationshipCursor, record, record.getFirstLoop(), RelationshipGroupLink.LOOP,
                            group -> reporter.forRelationshipGroup( group ).firstLoopRelationshipNotInUse(),
                            group -> reporter.forRelationshipGroup( group ).firstLoopRelationshipNotFirstInChain(),
                            group -> reporter.forRelationshipGroup( group ).firstLoopRelationshipOfOtherType(),
                            ( group, rel ) -> reporter.forRelationshipGroup( group ).firstLoopRelationshipDoesNotShareNodeWithGroup( rel ),
                            cursorTracer );
                }
            }
            localProgress.done();
        }
    }

    private void checkRelationshipGroupRelationshipLink( RecordRelationshipScanCursor relationshipCursor, RelationshipGroupRecord record, long relationshipId,
            RelationshipGroupLink relationshipGroupLink, Consumer<RelationshipGroupRecord> reportRelationshipNotInUse,
            Consumer<RelationshipGroupRecord> reportRelationshipNotFirstInChain, Consumer<RelationshipGroupRecord> reportRelationshipOfOtherType,
            BiConsumer<RelationshipGroupRecord,RelationshipRecord> reportNodeNotSharedWithGroup, PageCursorTracer cursorTracer )
    {
        if ( !NULL_REFERENCE.is( relationshipId ) )
        {
            relationshipCursor.single( relationshipId );
            if ( !relationshipCursor.next() )
            {
                reportRelationshipNotInUse.accept( record );
            }
            else
            {
                if ( !relationshipGroupLink.isFirstInChain( relationshipCursor ) )
                {
                    reportRelationshipNotFirstInChain.accept( record );
                }
                if ( relationshipCursor.getType() != record.getType() )
                {
                    reportRelationshipOfOtherType.accept( record );
                }

                boolean hasCorrectNode =
                        relationshipCursor.getFirstNode() == record.getOwningNode() || relationshipCursor.getSecondNode() == record.getOwningNode();
                if ( !hasCorrectNode )
                {
                    reportNodeNotSharedWithGroup.accept( record, context.recordLoader.relationship( relationshipCursor.getId(), cursorTracer ) );
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return String.format( "%s[highId:%d]", getClass().getSimpleName(), neoStores.getRelationshipGroupStore().getHighId() );
    }
}
