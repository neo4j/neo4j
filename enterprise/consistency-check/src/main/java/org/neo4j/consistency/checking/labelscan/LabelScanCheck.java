/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.labelscan;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.full.NodeInUseWithCorrectLabelsCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.api.direct.NodeLabelRange;

public class LabelScanCheck implements RecordCheck<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
{
    @Override
    public void check( LabelScanDocument record, CheckerEngine<LabelScanDocument,
            ConsistencyReport.LabelScanConsistencyReport> engine, RecordAccess records )
    {
        NodeLabelRange range = record.getNodeLabelRange();
        for ( long nodeId : range.nodes() )
        {
            engine.comparativeCheck( records.node( nodeId ), new NodeInUseWithCorrectLabelsCheck<LabelScanDocument,ConsistencyReport.LabelScanConsistencyReport>(
                    record.getNodeLabelRange().labels( nodeId ) ) );
        }
    }

    @Override
    public void checkChange( LabelScanDocument oldRecord, LabelScanDocument newRecord,
                             CheckerEngine<LabelScanDocument,
                                     ConsistencyReport.LabelScanConsistencyReport> engine, DiffRecordAccess records )
    {
        check( newRecord, engine, records );
    }
}
