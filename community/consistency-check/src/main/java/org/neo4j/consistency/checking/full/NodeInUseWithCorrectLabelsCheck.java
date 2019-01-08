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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.ArrayUtils;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.LabelChainWalker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static java.util.Arrays.sort;

public class NodeInUseWithCorrectLabelsCheck
        <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport.NodeInUseWithCorrectLabelsReport>
        implements ComparativeRecordChecker<RECORD, NodeRecord, REPORT>
{
    private final long[] indexLabels;
    private final boolean checkStoreToIndex;

    public NodeInUseWithCorrectLabelsCheck( long[] expectedLabels, boolean checkStoreToIndex )
    {
        this.checkStoreToIndex = checkStoreToIndex;
        this.indexLabels = sortAndDeduplicate( expectedLabels );
    }

    private static long[] sortAndDeduplicate( long[] labels )
    {
        if ( ArrayUtils.isNotEmpty( labels ) )
        {
            sort( labels );
            return PrimitiveLongCollections.deduplicate( labels );
        }
        return labels;
    }

    @Override
    public void checkReference( RECORD record, NodeRecord nodeRecord,
                                CheckerEngine<RECORD, REPORT> engine, RecordAccess records )
    {
        if ( nodeRecord.inUse() )
        {
            NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( nodeRecord );
            if ( nodeLabels instanceof DynamicNodeLabels )
            {
                DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
                long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
                RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
                ExpectedNodeLabelsChecker expectedNodeLabelsChecker = new ExpectedNodeLabelsChecker( nodeRecord );
                LabelChainWalker<RECORD,REPORT> checker = new LabelChainWalker<>( expectedNodeLabelsChecker );
                engine.comparativeCheck( firstRecordReference, checker );
                nodeRecord.getDynamicLabelRecords(); // I think this is empty in production
            }
            else
            {
                long[] storeLabels = nodeLabels.get( null );
                REPORT report = engine.report();
                validateLabelIds( nodeRecord, storeLabels, report );
            }
        }
        else if ( indexLabels.length != 0 )
        {
            engine.report().nodeNotInUse( nodeRecord );
        }
    }

    private void validateLabelIds( NodeRecord nodeRecord, long[] storeLabels, REPORT report )
    {
        storeLabels = sortAndDeduplicate( storeLabels );

        int indexLabelsCursor = 0;
        int storeLabelsCursor = 0;

        while ( indexLabelsCursor < indexLabels.length && storeLabelsCursor < storeLabels.length )
        {
            long indexLabel = indexLabels[indexLabelsCursor];
            long storeLabel = storeLabels[storeLabelsCursor];
            if ( indexLabel < storeLabel )
            {   // node store has a label which isn't in label scan store
                report.nodeDoesNotHaveExpectedLabel( nodeRecord, indexLabel );
                indexLabelsCursor++;
            }
            else if ( indexLabel > storeLabel )
            {   // label scan store has a label which isn't in node store
                reportNodeLabelNotInIndex( report, nodeRecord, storeLabel );
                storeLabelsCursor++;
            }
            else
            {   // both match
                indexLabelsCursor++;
                storeLabelsCursor++;
            }
        }

        while ( indexLabelsCursor < indexLabels.length )
        {
            report.nodeDoesNotHaveExpectedLabel( nodeRecord, indexLabels[indexLabelsCursor++] );
        }
        while ( storeLabelsCursor < storeLabels.length )
        {
            reportNodeLabelNotInIndex( report, nodeRecord, storeLabels[storeLabelsCursor] );
            storeLabelsCursor++;
        }
    }

    private void reportNodeLabelNotInIndex( REPORT report, NodeRecord nodeRecord, long storeLabel )
    {
        if ( checkStoreToIndex )
        {
            report.nodeLabelNotInIndex( nodeRecord, storeLabel );
        }
    }

    private class ExpectedNodeLabelsChecker implements
            LabelChainWalker.Validator<RECORD, REPORT>
    {
        private final NodeRecord nodeRecord;

        ExpectedNodeLabelsChecker( NodeRecord nodeRecord )
        {
            this.nodeRecord = nodeRecord;
        }

        @Override
        public void onRecordNotInUse( DynamicRecord dynamicRecord, CheckerEngine<RECORD, REPORT> engine )
        {
            // checked elsewhere
        }

        @Override
        public void onRecordChainCycle( DynamicRecord record, CheckerEngine<RECORD, REPORT> engine )
        {
            // checked elsewhere
        }

        @Override
        public void onWellFormedChain( long[] labelIds, CheckerEngine<RECORD, REPORT> engine, RecordAccess records )
        {
            validateLabelIds( nodeRecord, labelIds, engine.report() );
        }
    }
}
