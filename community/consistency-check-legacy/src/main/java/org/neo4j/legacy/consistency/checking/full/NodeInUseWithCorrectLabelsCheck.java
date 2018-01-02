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

import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.checking.LabelChainWalker;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.sort;

public class NodeInUseWithCorrectLabelsCheck
        <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport.NodeInUseWithCorrectLabelsReport>
        implements ComparativeRecordChecker<RECORD, NodeRecord, REPORT>
{
    private final long[] expectedLabels;

    public NodeInUseWithCorrectLabelsCheck( long[] expectedLabels )
    {
        this.expectedLabels = expectedLabels;
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
                engine.comparativeCheck( firstRecordReference,
                        new LabelChainWalker<RECORD, REPORT>
                                (new ExpectedNodeLabelsChecker( nodeRecord )) );
                nodeRecord.getDynamicLabelRecords(); // I think this is empty in production
            }
            else
            {
                long[] actualLabels = nodeLabels.get( null );
                REPORT report = engine.report();
                validateLabelIds( nodeRecord, actualLabels, report );
            }
        }
        else
        {
            engine.report().nodeNotInUse( nodeRecord );
        }
    }

    private void validateLabelIds( NodeRecord nodeRecord, long[] actualLabels, REPORT report )
    {
        sort(actualLabels);
        for ( long expectedLabel : expectedLabels )
        {
            int labelIndex = binarySearch( actualLabels, expectedLabel );
            if (labelIndex < 0)
            {
                report.nodeDoesNotHaveExpectedLabel( nodeRecord, expectedLabel );
            }
        }
    }

    private class ExpectedNodeLabelsChecker implements
            LabelChainWalker.Validator<RECORD, REPORT>
    {
        private final NodeRecord nodeRecord;

        public ExpectedNodeLabelsChecker( NodeRecord nodeRecord )
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
