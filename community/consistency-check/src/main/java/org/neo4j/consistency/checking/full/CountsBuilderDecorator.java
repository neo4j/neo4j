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
package org.neo4j.consistency.checking.full;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.Set;
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
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.internal.counts.CountsKey;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checking.cache.CacheSlots.NodeLabel.SLOT_IN_USE;
import static org.neo4j.consistency.checking.cache.CacheSlots.NodeLabel.SLOT_LABEL_FIELD;
import static org.neo4j.consistency.checking.full.NodeLabelReader.getListOfLabels;
import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class CountsBuilderDecorator extends CheckDecorator.Adapter
{
    private static final int WILDCARD = -1;
    private final MutableObjectLongMap<CountsKey> nodeCounts = new ObjectLongHashMap<>();
    private final MutableObjectLongMap<CountsKey> relationshipCounts = new ObjectLongHashMap<>();
    private final MultiPassAvoidanceCondition<NodeRecord> nodeCountBuildCondition;
    private final MultiPassAvoidanceCondition<RelationshipRecord> relationshipCountBuildCondition;
    private final CountsEntry.CheckAdapter nodeCountChecker = new NodeCountChecker();
    private final CountsEntry.CheckAdapter relationshipCountChecker = new RelationshipCountChecker();
    private final NodeStore nodeStore;
    private final StoreAccess storeAccess;

    CountsBuilderDecorator( StoreAccess storeAccess )
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

    public void checkCounts( CountsAccessor counts, final ConsistencyReporter reporter, ProgressMonitorFactory progressFactory, PageCursorTracer cursorTracer )
    {
        final int nodes = nodeCounts.size();
        final int relationships = relationshipCounts.size();
        final int total = nodes + relationships;
        final ProgressListener listener = progressFactory.singlePart( "Checking node and relationship counts", total );
        listener.started();
        counts.accept( new EntriesCheckerVisitor( reporter, cursorTracer, listener ), cursorTracer );
        nodeCounts.forEachKeyValue( ( key, count ) -> reporter.forCounts( new CountsEntry( key, 0 ), nodeCountChecker, cursorTracer ) );
        relationshipCounts.forEachKeyValue( ( key, count ) -> reporter.forCounts( new CountsEntry( key, 0 ), relationshipCountChecker, cursorTracer ) );
        listener.done();
    }

    private static class NodeCounts implements OwningRecordCheck<NodeRecord,NodeConsistencyReport>
    {
        private final RecordStore<NodeRecord> nodeStore;
        private final MutableObjectLongMap<CountsKey> counts;
        private final Predicate<NodeRecord> countUpdateCondition;
        private final OwningRecordCheck<NodeRecord,NodeConsistencyReport> inner;

        NodeCounts( RecordStore<NodeRecord> nodeStore, MutableObjectLongMap<CountsKey> counts,
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
                           RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( countUpdateCondition.test( record ) )
            {
                if ( record.inUse() )
                {
                    CacheAccess.Client client = records.cacheAccess().client();
                    client.putToCacheSingle( record.getId(), SLOT_IN_USE, 1 );
                    client.putToCacheSingle( record.getId(), SLOT_LABEL_FIELD, record.getLabelField() );
                    final Set<Long> labels = labelsFor( nodeStore, engine, records, record.getId(), cursorTracer );
                    synchronized ( counts )
                    {
                        counts.addToValue( nodeKey( WILDCARD ), 1 );
                        for ( long label : labels )
                        {
                            counts.addToValue( nodeKey( (int) label ), 1 );
                        }
                    }
                }
            }
            inner.check( record, engine, records, cursorTracer );
        }
    }

    private static class RelationshipCounts implements OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport>
    {
        private final NodeStore nodeStore;
        private final MutableObjectLongMap<CountsKey> counts;
        private final Predicate<RelationshipRecord> countUpdateCondition;
        private final OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> inner;

        RelationshipCounts( StoreAccess storeAccess, MutableObjectLongMap<CountsKey> counts,
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
                           RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( countUpdateCondition.test( record ) )
            {
                if ( record.inUse() )
                {
                    CacheAccess.Client cacheAccess = records.cacheAccess().client();
                    Set<Long> firstNodeLabels;
                    Set<Long> secondNodeLabels;
                    long firstLabelsField = cacheAccess.getFromCache( record.getFirstNode(), SLOT_LABEL_FIELD );
                    if ( NodeLabelsField.fieldPointsToDynamicRecordOfLabels( firstLabelsField ) )
                    {
                        firstNodeLabels = labelsFor( nodeStore, engine, records, record.getFirstNode(), cursorTracer );
                    }
                    else
                    {
                        firstNodeLabels = NodeLabelReader.getListOfLabels( firstLabelsField );
                    }
                    long secondLabelsField = cacheAccess.getFromCache( record.getSecondNode(), SLOT_LABEL_FIELD );
                    if ( NodeLabelsField.fieldPointsToDynamicRecordOfLabels( secondLabelsField ) )
                    {
                        secondNodeLabels = labelsFor( nodeStore, engine, records, record.getSecondNode(), cursorTracer );
                    }
                    else
                    {
                        secondNodeLabels = NodeLabelReader.getListOfLabels( secondLabelsField );
                    }
                    final int type = record.getType();
                    synchronized ( counts )
                    {
                        counts.addToValue( relationshipKey( WILDCARD, WILDCARD, WILDCARD ), 1 );
                        counts.addToValue( relationshipKey( WILDCARD, type, WILDCARD ), 1 );
                        for ( long firstLabel : firstNodeLabels )
                        {
                            counts.addToValue( relationshipKey( (int) firstLabel, WILDCARD, WILDCARD ), 1 );
                            counts.addToValue( relationshipKey( (int) firstLabel, type, WILDCARD ), 1 );
                        }
                        for ( long secondLabel : secondNodeLabels )
                        {
                            counts.addToValue( relationshipKey( WILDCARD, WILDCARD, (int) secondLabel ), 1 );
                            counts.addToValue( relationshipKey( WILDCARD, type, (int) secondLabel ), 1 );
                        }
                    }
                }
            }
            inner.check( record, engine, records, cursorTracer );
        }
    }

    private static class MultiPassAvoidanceCondition<T extends AbstractBaseRecord> implements Predicate<T>
    {
        // Stage which this condition is active, starting from 0, mimicking the CheckStage ordinal
        private final int activeStage;
        // The same thread updates this every time, the TaskExecutor. Other threads read it
        private volatile int stage = -1;

        MultiPassAvoidanceCondition( int activeStage )
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
                                        long nodeId, PageCursorTracer cursorTracer )
    {
        return getListOfLabels( nodeStore.getRecord( nodeId, nodeStore.newRecord(), FORCE, cursorTracer ), recordAccess, engine, cursorTracer );
    }

    private class NodeCountChecker extends CountsEntry.CheckAdapter
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records, PageCursorTracer cursorTracer )
        {
            final long expectedCount = nodeCounts.removeKeyIfAbsent( record.getCountsKey(), 0 );
            if ( expectedCount != record.getCount() )
            {
                engine.report().inconsistentNodeCount( expectedCount );
            }
        }
    }

    private class RelationshipCountChecker extends CountsEntry.CheckAdapter
    {
        @Override
        public void check( CountsEntry record,
                           CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                           RecordAccess records, PageCursorTracer cursorTracer )
        {
            final long expectedCount = relationshipCounts.removeKeyIfAbsent( record.getCountsKey(), 0 );
            if ( expectedCount != record.getCount() )
            {
                engine.report().inconsistentRelationshipCount( expectedCount );
            }
        }
    }

    private class EntriesCheckerVisitor extends CountsVisitor.Adapter
    {
        private final ConsistencyReporter reporter;
        private final PageCursorTracer cursorTracer;
        private final ProgressListener listener;

        private EntriesCheckerVisitor( ConsistencyReporter reporter, PageCursorTracer cursorTracer, ProgressListener listener )
        {
            this.reporter = reporter;
            this.cursorTracer = cursorTracer;
            this.listener = listener;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            reporter.forCounts( new CountsEntry( nodeKey( labelId ), count ), nodeCountChecker, cursorTracer );
            listener.add( 1 );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int relTypeId, int endLabelId, long count )
        {
            reporter.forCounts(
                    new CountsEntry( relationshipKey( startLabelId, relTypeId, endLabelId ), count ), relationshipCountChecker, cursorTracer );
            listener.add( 1 );
        }
    }
}
