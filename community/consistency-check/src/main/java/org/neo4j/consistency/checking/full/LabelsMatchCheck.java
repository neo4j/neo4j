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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.impl.store.record.NodeRecord;

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
        Set<Long> labelsFromNode = NodeLabelReader.getListOfLabels( record, records, engine );
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
}
