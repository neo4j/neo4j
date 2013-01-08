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
package org.neo4j.consistency.checking.incremental;

import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.kernel.impl.util.StringLogger;

public class IncrementalDiffCheck extends DiffCheck
{
    public IncrementalDiffCheck( StringLogger logger )
    {
        super( logger );
    }

    @Override
    public ConsistencySummaryStatistics execute( DiffStore diffs )
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        ConsistencyReporter reporter = new ConsistencyReporter( new DirectRecordAccess( diffs ),
                new InconsistencyReport( new InconsistencyMessageLogger( logger ), summary ) );
        diffs.applyToAll( new StoreProcessor( reporter ) );
        return summary;
    }
}
