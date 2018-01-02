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
package org.neo4j.legacy.consistency.checking.index;

import org.neo4j.legacy.consistency.checking.full.IndexCheck;
import org.neo4j.legacy.consistency.checking.full.RecordProcessor;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;
import org.neo4j.legacy.consistency.store.synthetic.IndexEntry;

public class IndexEntryProcessor implements RecordProcessor<Long>
{
    private final ConsistencyReporter reporter;
    private final IndexCheck indexCheck;

    public IndexEntryProcessor( ConsistencyReporter reporter, IndexCheck indexCheck )
    {
        this.reporter = reporter;
        this.indexCheck = indexCheck;
    }

    @Override
    public void process( Long nodeId )
    {
        reporter.forIndexEntry( new IndexEntry( nodeId ), indexCheck );
    }

    @Override
    public void close()
    {
    }
}
