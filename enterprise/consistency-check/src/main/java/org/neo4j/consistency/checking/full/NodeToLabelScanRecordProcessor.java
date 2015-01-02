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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

public class NodeToLabelScanRecordProcessor implements RecordProcessor<NodeRecord>
{
    private final ConsistencyReporter reporter;
    private final RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> nodeLabelCheck;
    private final LabelScanReader reader;

    public NodeToLabelScanRecordProcessor(
            ConsistencyReporter reporter, LabelScanStore labelScanStore )
    {
        this.reporter = reporter;
        this.reader = labelScanStore.newReader();
        this.nodeLabelCheck = new LabelsMatchCheck( reader );
    }

    @Override
    public void process( NodeRecord nodeRecord )
    {
        reporter.forNodeLabelMatch( nodeRecord, nodeLabelCheck );
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
