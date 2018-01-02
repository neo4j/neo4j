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
package org.neo4j.legacy.consistency.checking.full;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.MultiSet;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.checking.OwningRecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.synthetic.CountsEntry;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.legacy.consistency.checking.full.NodeLabelReader.getListOfLabels;

class CountsBuilderDecorator extends CheckDecorator.Adapter
{
    private static final int WILDCARD = -1;
    private final MultiSet<CountsKey> nodeCounts = new MultiSet<>();
    private final MultiSet<CountsKey> relationshipCounts = new MultiSet<>();
    private final Predicate<NodeRecord> nodeCountBuildCondition = new MultiPassAvoidanceCondition<>();
    private final Predicate<RelationshipRecord> relationshipCountBuildCondition = new MultiPassAvoidanceCondition<>();
    private final NodeStore nodeStore;
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

    public CountsBuilderDecorator( NodeStore nodeStore )
    {
        this.nodeStore = nodeStore;
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
        return new RelationshipCounts( nodeStore, relationshipCounts, relationshipCountBuildCondition, checker );
    }

    public void checkCounts( CountsAccessor counts, final ConsistencyReporter reporter, ProgressMonitorFactory
            progressFactory )
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
        private final NodeStore nodeStore;
        private final MultiSet<CountsKey> counts;
        private final Predicate<NodeRecord> countUpdateCondition;
        private final OwningRecordCheck<NodeRecord,NodeConsistencyReport> inner;

        public NodeCounts( NodeStore nodeStore, MultiSet<CountsKey> counts,
                           Predicate<NodeRecord> countUpdateCondition,
                           OwningRecordCheck<NodeRecord,NodeConsistencyReport> inner )
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
                    final Set<Long> labels = labelsFor( nodeStore, engine, records, record.getId() );
                    counts.add( nodeKey( WILDCARD ) );
                    for ( long label : labels )
                    {
                        counts.add( nodeKey( (int) label ) );
                    }
                }
            }
            inner.check( record, engine, records );
        }

        @Override
        public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                 CheckerEngine<NodeRecord,NodeConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            inner.checkChange( oldRecord, newRecord, engine, records );
        }
    }

    private static class RelationshipCounts implements OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport>
    {
        /** Don't support these counts at the moment so don't compute them */
        private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;
        private final NodeStore nodeStore;
        private final MultiSet<CountsKey> counts;
        private final Predicate<RelationshipRecord> countUpdateCondition;
        private final OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> inner;

        public RelationshipCounts( NodeStore nodeStore, MultiSet<CountsKey> counts,
                                   Predicate<RelationshipRecord> countUpdateCondition,
                                   OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> inner )
        {
            this.nodeStore = nodeStore;
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
            if (countUpdateCondition.test( record ))
            {
                if ( record.inUse() )
                {
                    final Set<Long> firstNodeLabels = labelsFor( nodeStore, engine, records, record.getFirstNode() );
                    final Set<Long> secondNodeLabels = labelsFor( nodeStore, engine, records, record.getSecondNode() );
                    final int type = record.getType();

                    counts.add( relationshipKey( WILDCARD, WILDCARD, WILDCARD ) );
                    counts.add( relationshipKey( WILDCARD, type, WILDCARD ) );
                    for ( long firstLabel : firstNodeLabels )
                    {
                        counts.add( relationshipKey( (int) firstLabel, WILDCARD, WILDCARD ) );
                        counts.add( relationshipKey( (int) firstLabel, type, WILDCARD ) );
                    }

                    for ( long secondLabel : secondNodeLabels )
                    {
                        counts.add( relationshipKey( WILDCARD, WILDCARD, (int) secondLabel ) );
                        counts.add( relationshipKey( WILDCARD, type, (int) secondLabel ) );
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
            inner.check( record, engine, records );
        }

        @Override
        public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                 CheckerEngine<RelationshipRecord,RelationshipConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            inner.checkChange( oldRecord, newRecord, engine, records );
        }
    }

    private static Set<Long> labelsFor( NodeStore nodeStore,
                                        CheckerEngine<? extends AbstractBaseRecord,? extends ConsistencyReport> engine,
                                        RecordAccess recordAccess,
                                        long nodeId )
    {
        return getListOfLabels( nodeStore.forceGetRecord( nodeId ), recordAccess, engine );
    }
}
