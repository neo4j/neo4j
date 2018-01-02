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

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.checking.index.IndexAccessors;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;

public class NodeToLabelIndexesProcessor implements RecordProcessor<NodeRecord>
{
    private final ConsistencyReporter reporter;
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeIndexCheck;

    public NodeToLabelIndexesProcessor( ConsistencyReporter reporter,
                                        IndexAccessors indexes,
                                        PropertyReader propertyReader )
    {
        this.reporter = reporter;
        this.nodeIndexCheck = new NodeCorrectlyIndexedCheck( indexes, propertyReader );
    }

    @Override
    public void process( NodeRecord nodeRecord )
    {
        reporter.forNode( nodeRecord, nodeIndexCheck );
    }

    @Override
    public void close()
    {
    }
}
