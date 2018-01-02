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

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.kernel.impl.store.record.IndexRule;

public class IndexCheck implements RecordCheck<IndexEntry, ConsistencyReport.IndexConsistencyReport>
{
    private final IndexRule indexRule;

    public IndexCheck( IndexRule indexRule )
    {
        this.indexRule = indexRule;
    }

    @Override
    public void check( IndexEntry record, CheckerEngine<IndexEntry, ConsistencyReport.IndexConsistencyReport> engine, RecordAccess records )
    {
        engine.comparativeCheck( records.node( record.getId() ),
                new NodeInUseWithCorrectLabelsCheck<IndexEntry,ConsistencyReport.IndexConsistencyReport>(new long[] {indexRule.getLabel()}) );
    }
}
