/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheAccess.Client;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.consistency.store.DirectRecordReference;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.NEXT;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.PREV;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_REFERENCE;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_RELATIONSHIP_ID;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_SOURCE_OR_TARGET;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SOURCE;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.TARGET;
import static org.neo4j.helpers.ArrayUtil.union;

public class RelationshipRecordCheck extends
        PrimitiveRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>
{
    public RelationshipRecordCheck()
    {
        this( RelationshipTypeField.RELATIONSHIP_TYPE, NodeField.SOURCE, RelationshipField.SOURCE_PREV,
                RelationshipField.SOURCE_NEXT, NodeField.TARGET, RelationshipField.TARGET_PREV,
                RelationshipField.TARGET_NEXT );
    }

    @SafeVarargs
    RelationshipRecordCheck(
            RecordField<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>... fields )
    {
        super( fields );
    }

    public static RelationshipRecordCheck relationshipRecordCheckForwardPass()
    {
        return new RelationshipRecordCheck( RelationshipTypeField.RELATIONSHIP_TYPE );
    }

    @SafeVarargs
    public static RelationshipRecordCheck relationshipRecordCheckBackwardPass(
            RecordField<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>... additional )
    {
        return new RelationshipRecordCheck( union(
                ArrayUtil.<RecordField<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>>array(
                        NodeField.SOURCE, NodeField.TARGET ), additional ) );
    }

    public static RelationshipRecordCheck relationshipRecordCheckSourceChain()
    {
        return new RelationshipRecordCheck( RelationshipField.SOURCE_NEXT,
                RelationshipField.SOURCE_PREV, RelationshipField.TARGET_NEXT, RelationshipField.TARGET_PREV,
                RelationshipField.CACHE_VALUES );
    }

    enum RelationshipTypeField
            implements
            RecordField<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord,RelationshipTypeTokenRecord,ConsistencyReport.RelationshipConsistencyReport>
    {
        RELATIONSHIP_TYPE;
        @Override
        public void checkConsistency( RelationshipRecord record,
                                      CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
            if ( record.getType() < 0 )
            {
                engine.report().illegalRelationshipType();
            }
            else
            {
                engine.comparativeCheck( records.relationshipType( record.getType() ), this );
            }
        }

        @Override
        public long valueFrom( RelationshipRecord record )
        {
            return record.getType();
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipTypeTokenRecord referred,
                                    CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().relationshipTypeNotInUse( referred );
            }
        }
    }

    enum RelationshipField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        SOURCE_PREV( NodeField.SOURCE )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourcePrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                    RelationshipRecord relationship )
            {
                report.sourcePrevDoesNotReferenceBack( relationship );
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }

            @Override
            RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                    CacheAccess.Client cacheAccess )
            {
                if ( cacheAccess.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE )
                {
                    rel.setFirstNextRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                else
                {
                    rel.setSecondNextRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                return rel;
            }

            @Override
            void linkChecked( CacheAccess.Client cacheAccess )
            {
                cacheAccess.incAndGetCount( Counts.Type.relSourcePrevCheck );
            }
        },
        SOURCE_NEXT( NodeField.SOURCE )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourceNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                    RelationshipRecord relationship )
            {
                report.sourceNextDoesNotReferenceBack( relationship );
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }

            @Override
            RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                    CacheAccess.Client cacheAccess )
            {
                if ( cacheAccess.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE )
                {
                    rel.setFirstPrevRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                else
                {
                    rel.setSecondPrevRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                return rel;
            }

            @Override
            void linkChecked( CacheAccess.Client cacheAccess )
            {
                cacheAccess.incAndGetCount( Counts.Type.relSourceNextCheck );
            }
        },
        TARGET_PREV( NodeField.TARGET )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetPrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                    RelationshipRecord relationship )
            {
                report.targetPrevDoesNotReferenceBack( relationship );
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }

            @Override
            RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                    CacheAccess.Client cacheAccess )
            {
                if ( cacheAccess.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE )
                {
                    rel.setFirstNextRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                else
                {
                    rel.setSecondNextRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                return rel;
            }

            @Override
            void linkChecked( CacheAccess.Client cacheAccess )
            {
                cacheAccess.incAndGetCount( Counts.Type.relTargetPrevCheck );
            }
        },
        TARGET_NEXT( NodeField.TARGET )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                    RelationshipRecord relationship )
            {
                report.targetNextDoesNotReferenceBack( relationship );
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }

            @Override
            RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                    CacheAccess.Client cacheAccess )
            {
                if ( cacheAccess.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE )
                {
                    rel.setFirstPrevRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                else
                {
                    rel.setSecondPrevRel( cacheAccess.getFromCache( nodeId, SLOT_REFERENCE ) );
                }
                return rel;
            }

            @Override
            void linkChecked( CacheAccess.Client cacheAccess )
            {
                cacheAccess.incAndGetCount( Counts.Type.relTargetNextCheck );
            }
        },
        CACHE_VALUES( null )
        {
            @Override
            public long valueFrom( RelationshipRecord record )
            {
                return 0;
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return false;
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return 0;
            }

            @Override
            void otherNode( RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            }

            @Override
            void noBackReference( RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            }

            @Override
            RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                    CacheAccess.Client cacheAccess )
            {
                return null;
            }

            @Override
            public void checkConsistency( RelationshipRecord relationship,
                    CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                    RecordAccess records )
            {
                if ( !relationship.inUse() )
                {
                    return;
                }

                CacheAccess.Client cacheAccess = records.cacheAccess().client();
                /*
                 * save to cache the information in this relationship that will be
                 * referred in future, i.e., in the forward scan, these are the link ids
                 * having value greater than current one and in the backward scan, these
                 * are the link ids having value lower than the current one. Choose the
                 * lower of two in case of forward scan, and greater of two in backward
                 * scan. Save ONLY when the cache slot is available. Otherwise, i.e., it
                 * contains cached information, check if the information from current
                 * relationship will be utilized sooner. If so, update the cached
                 * information.
                 */
                boolean cache1Free = cacheAccess.getFromCache( relationship.getFirstNode(), SLOT_RELATIONSHIP_ID ) == -1;
                boolean cache2Free = cacheAccess.getFromCache( relationship.getSecondNode(), SLOT_RELATIONSHIP_ID ) == -1;

                if ( records.cacheAccess().isForward() )
                {
                    if ( cacheAccess.withinBounds( relationship.getFirstNode() ) )
                    {
                        cacheAccess.putToCache( relationship.getFirstNode(), SOURCE, PREV,
                                relationship.getId(), relationship.getFirstPrevRel() );
                        updateCacheCounts( cache1Free, cacheAccess );
                    }
                    if ( cacheAccess.withinBounds( relationship.getSecondNode() ) )
                    {
                        cacheAccess.putToCache( relationship.getSecondNode(), TARGET, PREV,
                                relationship.getId(), relationship.getSecondPrevRel() );
                        updateCacheCounts( cache2Free, cacheAccess );
                    }
                }
                else
                {
                    if ( cacheAccess.withinBounds( relationship.getFirstNode() ) )
                    {
                        cacheAccess.putToCache( relationship.getFirstNode(), SOURCE, NEXT,
                                relationship.getId(), relationship.getFirstNextRel() );
                        updateCacheCounts( cache1Free, cacheAccess );
                    }
                    if ( cacheAccess.withinBounds( relationship.getSecondNode() ) )
                    {
                        cacheAccess.putToCache( relationship.getSecondNode(), TARGET, NEXT,
                                relationship.getId(), relationship.getSecondNextRel() );
                        updateCacheCounts( cache2Free, cacheAccess );
                    }
                }
            }

            private void updateCacheCounts( boolean free, Client cacheAccess )
            {
                if ( !free )
                {
                    cacheAccess.incAndGetCount( Counts.Type.overwrite );
                }
                else
                {
                    cacheAccess.incAndGetCount( Counts.Type.activeCache );
                }
            }

            @Override
            void linkChecked( CacheAccess.Client cacheAccess )
            {
                cacheAccess.incAndGetCount( Counts.Type.relCacheCheck );
            }
        };
        protected final NodeField NODE;

        RelationshipField( NodeField node )
        {
            this.NODE = node;
        }

        private RecordReference<RelationshipRecord> buildFromCache( RelationshipRecord relationship, long reference,
                long nodeId, RecordAccess records )
        {
            CacheAccess.Client cacheAccess = records.cacheAccess().client();
            if ( !cacheAccess.withinBounds( nodeId ) )
            {
                // another thread will visit this record, so skip
                cacheAccess.incAndGetCount( Counts.Type.correctSkipCheck );
                return RecordReference.SkippingReference.skipReference();
            }
            if ( reference != cacheAccess.getFromCache( nodeId, SLOT_RELATIONSHIP_ID ) )
            {
                if ( referenceShouldBeSkipped( relationship, reference, records ) )
                {
                    // wrong direction, so skip
                    cacheAccess.incAndGetCount( Counts.Type.correctSkipCheck );
                    return RecordReference.SkippingReference.skipReference();
                }
                //these are "bad links", and hopefully few. So, get the real ones anyway.
                cacheAccess.incAndGetCount( Counts.Type.missCheck );
                return records.relationship( reference );
            }
            // now, use cached info to build a fake relationship, but with partial real values that had been cached before
            RelationshipRecord rel = new RelationshipRecord( reference );

            // We use "created" flag here. Consistency checking code revolves around records and so
            // even in scenarios where records are built from other sources, f.ex half-and-purpose-built from cache,
            // this flag is used to signal that the real record needs to be read in order to be used as a general
            // purpose record.
            rel.setCreated();
            if ( cacheAccess.getFromCache( nodeId, SLOT_SOURCE_OR_TARGET ) == SOURCE )
            {
                rel.setFirstNode( nodeId );
            }
            else
            {
                rel.setSecondNode( nodeId );
            }
            rel = populateRelationshipFromCache( nodeId, rel, cacheAccess );
            return new DirectRecordReference<>( rel, records );
        }

        private boolean referenceShouldBeSkipped( RelationshipRecord relationship, long reference,
                RecordAccess records )
        {
            return (records.cacheAccess().isForward() && reference > relationship.getId())
               || (!records.cacheAccess().isForward() && reference < relationship.getId());
        }

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
            /*
             * The algorithm for fast consistency check does 2 passes over the relationship store - one forward and one backward.
             * In both passes, typically the cached information is used build the referred relationship instead of going to disk.
             * This is what minimizes the random access to disk, but it is guaranteed that the cached information was always got
             * from disk at an appropriate opportunity and all links are correctly checked with right data.
             * In the forward pass, only the previous relationship information is cached and hence only the next information can
             * be checked, while in backward pass only the next information is cached allowing checking of previous
             */
            CacheAccess.Client cacheAccess = records.cacheAccess().client();
            if ( !endOfChain( relationship ) )
            {
                RecordReference<RelationshipRecord> referred = null;
                long reference = valueFrom( relationship );
                long nodeId = -1;
                if ( records.shouldCheck( reference, MultiPassStore.RELATIONSHIPS ) )
                {
                    nodeId = NODE == NodeField.SOURCE ? relationship.getFirstNode() : relationship.getSecondNode();
                    if ( Record.NO_NEXT_RELATIONSHIP.is( cacheAccess.getFromCache( nodeId, SLOT_RELATIONSHIP_ID ) ) )
                    {
                        referred = RecordReference.SkippingReference.skipReference();
                        cacheAccess.incAndGetCount( Counts.Type.noCacheSkip );
                    }
                    else
                    {
                        referred = buildFromCache( relationship, reference, nodeId, records );
                        if ( referred == RecordReference.SkippingReference.<RelationshipRecord>skipReference() )
                        {
                            cacheAccess.incAndGetCount( Counts.Type.skipCheck );
                        }
                    }
                }
                else
                {
                    if ( referenceShouldBeSkipped( relationship, reference, records ) )
                    {
                        referred = RecordReference.SkippingReference.skipReference();
                    }
                }
                engine.comparativeCheck( referred, this );
                if ( referred != RecordReference.SkippingReference.<RelationshipRecord>skipReference() )
                {
                    cacheAccess.incAndGetCount( Counts.Type.checked );
                    linkChecked( cacheAccess );
                }
            }
            else
            {
                cacheAccess.incAndGetCount( Counts.Type.checked );
                linkChecked( cacheAccess );
            }
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipRecord referred,
                                    CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
            NodeField field = NodeField.select( referred, node( record ) );
            if ( field == null )
            {
                otherNode( engine.report(), referred );
            }
            else
            {
                CacheAccess.Client cacheAccess = records.cacheAccess().client();
                if ( other( field, referred ) != record.getId() )
                {
                    // We use "created" flag here. Consistency checking code revolves around records and so
                    // even in scenarios where records are built from other sources, f.ex half-and-purpose-built from cache,
                    // this flag is used to signal that the real record needs to be read in order to be used as a general
                    // purpose record.
                    if ( referred.isCreated() )
                    {
                        //get the actual record and check again
                        RecordReference<RelationshipRecord> refRel = records.relationship( referred.getId() );
                        referred = (RelationshipRecord) ((DirectRecordReference) refRel).record();
                        checkReference( record, referred, engine, records );
                        cacheAccess.incAndGetCount( Counts.Type.skipBackup );
                    }
                    else
                    {
                        cacheAccess.incAndGetCount( Counts.Type.checkErrors );
                        noBackReference( engine == null ? null : engine.report(), referred );
                    }
                }
                else
                {   // successfully checked
                    // clear cache only if cache is used - meaning referred was built using cache.
                    if ( referred.isCreated() )
                    {
                        cacheAccess.clearCache( node( record ) );
                    }
                }
            }
        }

        abstract boolean endOfChain( RelationshipRecord record );

        abstract long other( NodeField field, RelationshipRecord relationship );

        abstract void otherNode( ConsistencyReport.RelationshipConsistencyReport report,
                                 RelationshipRecord relationship );

        abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                       RelationshipRecord relationship );

        abstract RelationshipRecord populateRelationshipFromCache( long nodeId, RelationshipRecord rel,
                CacheAccess.Client cacheAccess );

        abstract void linkChecked( CacheAccess.Client cacheAccess );

        private long node( RelationshipRecord relationship )
        {
            return NODE.valueFrom( relationship );
        }
    }
}
