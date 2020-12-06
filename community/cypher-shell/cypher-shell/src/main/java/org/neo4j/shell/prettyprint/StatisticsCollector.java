/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.prettyprint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.shell.cli.Format;

public class StatisticsCollector
{
    private Format format;

    public StatisticsCollector( @Nonnull Format format )
    {
        this.format = format;
    }

    public String collect( @Nonnull ResultSummary summary )
    {
        if ( Format.VERBOSE == format )
        {
            return collectStatistics( summary );
        }
        else
        {
            return "";
        }
    }

    private String collectStatistics( @Nonnull ResultSummary summary )
    {
        List<String> statistics = new ArrayList<>();
        SummaryCounters counters = summary.counters();
        if ( counters == null )
        {
            return "";
        }
        if ( counters.nodesCreated() != 0 )
        {
            statistics.add( String.format( "Added %d nodes", counters.nodesCreated() ) );
        }
        if ( counters.nodesDeleted() != 0 )
        {
            statistics.add( String.format( "Deleted %d nodes", counters.nodesDeleted() ) );
        }
        if ( counters.relationshipsCreated() != 0 )
        {
            statistics.add( String.format( "Created %d relationships", counters.relationshipsCreated() ) );
        }
        if ( counters.relationshipsDeleted() != 0 )
        {
            statistics.add( String.format( "Deleted %d relationships", counters.relationshipsDeleted() ) );
        }
        if ( counters.propertiesSet() != 0 )
        {
            statistics.add( String.format( "Set %d properties", counters.propertiesSet() ) );
        }
        if ( counters.labelsAdded() != 0 )
        {
            statistics.add( String.format( "Added %d labels", counters.labelsAdded() ) );
        }
        if ( counters.labelsRemoved() != 0 )
        {
            statistics.add( String.format( "Removed %d labels", counters.labelsRemoved() ) );
        }
        if ( counters.indexesAdded() != 0 )
        {
            statistics.add( String.format( "Added %d indexes", counters.indexesAdded() ) );
        }
        if ( counters.indexesRemoved() != 0 )
        {
            statistics.add( String.format( "Removed %d indexes", counters.indexesRemoved() ) );
        }
        if ( counters.constraintsAdded() != 0 )
        {
            statistics.add( String.format( "Added %d constraints", counters.constraintsAdded() ) );
        }
        if ( counters.constraintsRemoved() != 0 )
        {
            statistics.add( String.format( "Removed %d constraints", counters.constraintsRemoved() ) );
        }
        return statistics.stream().collect( Collectors.joining( ", " ) );
    }
}
