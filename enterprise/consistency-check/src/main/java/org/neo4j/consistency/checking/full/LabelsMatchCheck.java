/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.LabelChainWalker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;

public class LabelsMatchCheck implements
        RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport>
{
    private final LabelScanReader labelScanReader;

    public LabelsMatchCheck( LabelScanReader labelScanReader )
    {
        this.labelScanReader = labelScanReader;
    }

    @Override
    public void check( NodeRecord record, CheckerEngine<NodeRecord, ConsistencyReport.LabelsMatchReport> engine,
                       RecordAccess records )
    {
        Set<Long> labelsFromNode = getListOfLabels( record, records, engine );
        Iterator<Long> labelsFromLabelScanStore = labelScanReader.labelsForNode( record.getId() );

        while ( labelsFromLabelScanStore.hasNext() )
        {
            labelsFromNode.remove( labelsFromLabelScanStore.next() );
        }

        for ( Long labelId : labelsFromNode )
        {
            engine.report().nodeLabelNotInIndex( record, labelId );
        }
    }

    @Override
    public void checkChange( NodeRecord oldRecord, NodeRecord newRecord, CheckerEngine<NodeRecord, ConsistencyReport.LabelsMatchReport> engine, DiffRecordAccess records )
    {
        check( newRecord, engine, records );
    }
    
    private Set<Long> getListOfLabels( NodeRecord nodeRecord, RecordAccess records, CheckerEngine<NodeRecord,
            ConsistencyReport.LabelsMatchReport> engine )
    {
        final Set<Long> labels = new HashSet<>();

        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( nodeRecord );
        if ( nodeLabels instanceof DynamicNodeLabels )
        {

            DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
            long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
            RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
            engine.comparativeCheck( firstRecordReference,
                    new LabelChainWalker<>(
                            new LabelChainWalker.Validator<NodeRecord, ConsistencyReport.LabelsMatchReport>()
                            {
                                @Override
                                public void onRecordNotInUse( DynamicRecord dynamicRecord, CheckerEngine<NodeRecord,
                                        ConsistencyReport.LabelsMatchReport> engine )
                                {
                                }

                                @Override
                                public void onRecordChainCycle( DynamicRecord record, CheckerEngine<NodeRecord,
                                        ConsistencyReport.LabelsMatchReport> engine )
                                {
                                }

                                @Override
                                public void onWellFormedChain( long[] labelIds, CheckerEngine<NodeRecord,
                                        ConsistencyReport.LabelsMatchReport> engine, RecordAccess records )
                                {
                                    copyToSet( labelIds, labels );

                                }
                            } ) );
        }
        else
        {
            copyToSet( nodeLabels.get( null ), labels );
        }

        return labels;
    }

    private void copyToSet( long[] array, Set<Long> set )
    {
        for ( long labelId : array )
        {
            set.add( labelId );
        }
    }
}
