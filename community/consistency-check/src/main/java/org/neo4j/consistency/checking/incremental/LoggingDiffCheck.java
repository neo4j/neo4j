/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.incremental;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.kernel.impl.util.StringLogger;

public class LoggingDiffCheck extends DiffCheck
{
    private final DiffCheck checker;

    public LoggingDiffCheck( DiffCheck checker, StringLogger logger )
    {
        super( logger );
        this.checker = checker;
    }

    @Override
    public ConsistencySummaryStatistics execute( DiffStore diffs ) throws ConsistencyCheckIncompleteException
    {
        return checker.execute( diffs );
    }

    @Override
    protected void verify( final DiffStore diffs, ConsistencySummaryStatistics summary )
    {
        if ( !summary.isConsistent() )
        {
            logger.logMessage( "Inconsistencies found: " + summary );
            // TODO: log all changes by the transaction
        }
    }
}
