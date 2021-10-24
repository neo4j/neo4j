/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.v44.runtime;

import java.time.Clock;

import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.bolt.v4.runtime.CypherAdapterStreamV4;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.values.virtual.MapValueBuilder;

public class CypherAdapterStreamV44 extends CypherAdapterStreamV4
{
    public CypherAdapterStreamV44( QueryExecution delegate, BoltAdapterSubscriber subscriber,
                                   Clock clock, String databaseName )
    {
        super( delegate, subscriber, clock, databaseName );
    }

    @Override
    public MapValueBuilder queryStats( QueryStatistics queryStatistics )
    {
        var existingStatsBuilder = super.queryStats( queryStatistics );
        addIfTrue( existingStatsBuilder,"contains-updates", queryStatistics.containsUpdates() );
        return existingStatsBuilder;
    }

    @Override
    public MapValueBuilder systemQueryStats( QueryStatistics queryStatistics )
    {
        var existingStatsBuilder = super.systemQueryStats( queryStatistics );
        addIfTrue( existingStatsBuilder, "contains-system-updates", queryStatistics.containsSystemUpdates() );
        return existingStatsBuilder;
    }
}
