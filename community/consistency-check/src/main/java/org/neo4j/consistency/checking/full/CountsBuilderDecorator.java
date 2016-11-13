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
package org.neo4j.consistency.checking.full;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.OwningRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.helpers.collection.MultiSet;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checking.cache.CacheSlots.NodeLabel.SLOT_IN_USE;
import static org.neo4j.consistency.checking.cache.CacheSlots.NodeLabel.SLOT_LABEL_FIELD;
import static org.neo4j.consistency.checking.full.NodeLabelReader.getListOfLabels;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class CountsBuilderDecorator extends CheckDecorator.Adapter
{
    private static final int WILDCARD = -1;
    private final MultiSet<CountsKey> nodeCounts = new MultiSet<>();
    private final MultiSet<CountsKey> relationshipCounts = new MultiSet<>();
    private final MultiPassAvoidanceCondition<NodeRecord> nodeCountBuildCondition;
    private final MultiPassAvoidanceCondition<RelationshipRecord> relationshipCountBuildCondition;
    private final NodeStore nodeStore;
    private final StoreAccess storeAccess;
    private final CountsEntry.CheckAdapter CHECK_NODE_COUNT = new CountsEntry.CheckAdapter()
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records )
        {
            final long expectedCount = nodeCounts.count( record.getCountsKey() );
            if ( expectedCount != record.getCount() )
            {
                engine.report().inconsistentNodeCount( expectedCount );
            }
        }
    };
    private final CountsEntry.CheckAdapter CHECK_RELATIONSHIP_COUNT = new CountsEntry.CheckAdapter()
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records )
        {
            final long expectedCount = relationshipCounts.count( record.getCountsKey() );
            if ( expectedCount != record.getCount() )
            {
                engine.report().inconsistentRelationshipCount( expectedCount );
            }
        }
    };
    private final CountsEntry.CheckAdapter CHECK_NODE_KEY_COUNT = new CountsEntry.CheckAdapter()
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records )
        {
            final int expectedCount = nodeCounts.uniqueSize();
            if ( record.getCount() != expectedCount )
            {
                engine.report().inconsistentNumberOfNodeKeys( expectedCount );
            }
        }
    };
    private final CountsEntry.CheckAdapter CHECK_RELATIONSHIP_KEY_COUNT = new CountsEntry.CheckAdapter()
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records )
        {
            final int expectedCount = relationshipCounts.uniqueSize();
            if ( record.getCount() != expectedCount )
            {
                engine.report().inconsistentNumberOfRelationshipKeys( expectedCount );
            }
        }
    };

    public CountsBuilderDecorator( StoreAccess storeAccess )
    {
        this.storeAccess = storeAccess;
        this.nodeStore = storeAccess.getRawNeoStores().getNodeStore();
        this.nodeCountBuildCondition = new MultiPassAvoidanceCondition<>( 0 );
        this.relationshipCountBuildCondition = new MultiPassAvoidanceCondition<>( 1 );
    }

    @Override
    public void prepare()
    {
        this.nodeCountBuildCondition.prepare();
        this.relationshipCountBuildCondition.prepare();
    }

    @Override
    public OwningRecordCheck<NodeRecord,NodeConsistencyReport> decorateNodeChecker(
            OwningRecordCheck<NodeRecord,NodeConsistencyReport> checker )
    {
        return new NodeCounts( nodeStore, nodeCounts, nodeCountBuildCondition, checker );
    }

    @Override
    public OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> decorateRelationshipChecker(
            OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> checker )
    {
        return new RelationshipCounts( storeAccess, relationshipCounts, relationshipCountBuildCondition, checker );
    }

    public void checkCounts( CountsAccessor counts, final ConsistencyReporter reporter,
            ProgressMonitorFactory progressFactory )
    {
        final int nodes = nodeCounts.uniqueSize();
        final int relationships = relationshipCounts.uniqueSize();
        final int total = nodes + relationships;
        final AtomicInteger nodeEntries = new AtomicInteger( 0 );
        final AtomicInteger relationshipEntries = new AtomicInteger( 0 );
        final ProgressListener listener = progressFactory.singlePart( "Checking node and relationship counts", total );
        listener.started();
        counts.accept( new CountsVisitor.Adapter()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                nodeEntries.incrementAndGet();
                reporter.forCounts( new CountsEntry( nodeKey( labelId ), count ), CHECK_NODE_COUNT );
                listener.add( 1 );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int relTypeId, int endLabelId, long count )
            {
                relationshipEntries.incrementAndGet();
                reporter.forCounts(
                        new CountsEntry( relationshipKey( startLabelId, relTypeId, endLabelId ), count ),
                        CHECK_RELATIONSHIP_COUNT );
                listener.add( 1 );
            }
        } );
        reporter.forCounts(
                new CountsEntry( nodeKey( WILDCARD ), nodeEntries.get() ), CHECK_NODE_KEY_COUNT );
        reporter.forCounts(
                new CountsEntry( relationshipKey( WILDCARD, WILDCARD, WILDCARD ),
                        relationshipEntries.get() ), CHECK_RELATIONSHIP_KEY_COUNT );
        listener.done();
    }

    private static class NodeCounts implements OwningRecordCheck<NodeRecord,NodeConsistencyReport>
    {
        private final RecordStore<NodeRecord> nodeStore;
        private final MultiSet<CountsKey> counts;
        private final Predicate<NodeRecord> countUpdateCondition;
        private final OwningRecordCheck<NodeRecord,NodeConsistencyReport> inner;

        public NodeCounts( RecordStore<NodeRecord> nodeStore, MultiSet<CountsKey> counts,
                Predicate<NodeRecord> countUpdateCondition, OwningRecordCheck<NodeRecord,NodeConsistencyReport> inner )
        {
            this.nodeStore = nodeStore;
            this.counts = counts;
            this.countUpdateCondition = countUpdateCondition;
            this.inner = inner;
        }

        @Override
        public ComparativeRecordChecker<NodeRecord,PrimitiveRecord,NodeConsistencyReport> ownerCheck()
        {
            return inner.ownerCheck();
        }

        @Override
        public void check( NodeRecord record,
                           CheckerEngine<NodeRecord,NodeConsistencyReport> engine,
                           RecordAccess records )
        {
            if ( countUpdateCondition.test( record ) )
            {
                if ( record.inUse() )
                {
                    CacheAccess.Client client = records.cacheAccess().client();
                    client.putToCacheSingle( record.getId(), SLOT_IN_USE, 1 );
                    client.putToCacheSingle( record.getId(), SLOT_LABEL_FIELD, record.getLabelField() );
                    final Set<Long> labels = labelsFor( nodeStore, engine, records, record.getId() );
                    synchronized ( counts )
                    {
                        counts.add( nodeKey( WILDCARD ) );
                        for ( long label : labels )
                        {
                            counts.add( nodeKey( (int) label ) );
                        }
                    }
                }
            }
            inner.check( record, engine, records );
        }
    }

    private static class RelationshipCounts implements OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport>
    {
        /** Don't support these counts at the moment so don't compute them */
        private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
        private final NodeStore nodeStore;
        private final MultiSet<CountsKey> counts;
        private final Predicate<RelationshipRecord> countUpdateCondition;
        private final OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> inner;

        public RelationshipCounts( StoreAccess storeAccess, MultiSet<CountsKey> counts,
                                   Predicate<RelationshipRecord> countUpdateCondition,
                                   OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> inner )
        {
            this.nodeStore = storeAccess.getRawNeoStores().getNodeStore();
            this.counts = counts;
            this.countUpdateCondition = countUpdateCondition;
            this.inner = inner;
        }

        @Override
        public ComparativeRecordChecker<RelationshipRecord,PrimitiveRecord,RelationshipConsistencyReport> ownerCheck()
        {
            return inner.ownerCheck();
        }

        @Override
        public void check( RelationshipRecord record,
                           CheckerEngine<RelationshipRecord,RelationshipConsistencyReport> engine,
                           RecordAccess records )
        {
            if ( countUpdateCondition.test( record ) )
            {
                if ( record.inUse() )
                {
                    CacheAccess.Client cacheAccess = records.cacheAccess().client();
                    Set<Long> firstNodeLabels = null, secondNodeLabels = null;
                    long firstLabelsField = cacheAccess.getFromCache( record.getFirstNode(), SLOT_LABEL_FIELD );
                    if ( NodeLabelsField.fieldPointsToDynamicRecordOfLabels( firstLabelsField ) )
                    {
                        firstNodeLabels = labelsFor( nodeStore, engine, records, record.getFirstNode() );
                    }
                    else
                    {
                        firstNodeLabels = NodeLabelReader.getListOfLabels( firstLabelsField );
                    }
                    long secondLabelsField = cacheAccess.getFromCache( record.getSecondNode(), SLOT_LABEL_FIELD );
                    if ( NodeLabelsField.fieldPointsToDynamicRecordOfLabels( secondLabelsField ) )
                    {
                        secondNodeLabels = labelsFor( nodeStore, engine, records, record.getSecondNode() );
                    }
                    else
                    {
                        secondNodeLabels = NodeLabelReader.getListOfLabels( secondLabelsField );
                    }
                    final int type = record.getType();
                    synchronized ( counts )
                    {
                        counts.add( relationshipKey( WILDCARD, WILDCARD, WILDCARD ) );
                        counts.add( relationshipKey( WILDCARD, type, WILDCARD ) );
                        if ( firstNodeLabels != null )
                        {
                            for ( long firstLabel : firstNodeLabels )
                            {
                                counts.add( relationshipKey( (int) firstLabel, WILDCARD, WILDCARD ) );
                                counts.add( relationshipKey( (int) firstLabel, type, WILDCARD ) );
                            }
                        }
                        if ( secondNodeLabels != null )
                        {
                            for ( long secondLabel : secondNodeLabels )
                            {
                                counts.add( relationshipKey( WILDCARD, WILDCARD, (int) secondLabel ) );
                                counts.add( relationshipKey( WILDCARD, type, (int) secondLabel ) );
                            }
                        }
                        if ( COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS )
                        {
                            for ( long firstLabel : firstNodeLabels )
                            {
                                for ( long secondLabel : secondNodeLabels )
                                {
                                    counts.add( relationshipKey( (int) firstLabel, WILDCARD, (int) secondLabel ) );
                                    counts.add( relationshipKey( (int) firstLabel, type, (int) secondLabel ) );
                                }
                            }
                        }
                    }
                }
            }
            inner.check( record, engine, records );
        }
    }

    private static class MultiPassAvoidanceCondition<T extends AbstractBaseRecord> implements Predicate<T>
    {
        // Stage which this condition is active, starting from 0, mimicing the CheckStage ordinal
        private final int activeStage;
        // The same thread updates this every time, the TaskExecutor. Other threads read it
        private volatile int stage = -1;

        public MultiPassAvoidanceCondition( int activeStage )
        {
            this.activeStage = activeStage;
        }

        public void prepare()
        {
            stage++;
        }

        @Override
        public boolean test( T record )
        {
            return stage == activeStage;
        }
    }

    private static Set<Long> labelsFor( RecordStore<NodeRecord> nodeStore,
                                        CheckerEngine<? extends AbstractBaseRecord,? extends ConsistencyReport> engine,
                                        RecordAccess recordAccess,
                                        long nodeId )
    {
        return getListOfLabels( nodeStore.getRecord( nodeId, nodeStore.newRecord(), FORCE ), recordAccess, engine );
    }
}
