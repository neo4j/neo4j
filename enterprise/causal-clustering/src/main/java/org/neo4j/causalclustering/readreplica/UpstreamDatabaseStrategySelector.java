/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.readreplica;


import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.neo4j.causalclustering.identity.MemberId;

public class UpstreamDatabaseStrategySelector
{
    private LinkedList<UpstreamDatabaseSelectionStrategy> strategies = new LinkedList<>();

    UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        this( defaultStrategy, null );
    }

    UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy,
            Iterable<UpstreamDatabaseSelectionStrategy> otherStrategies )
    {
        strategies.push( defaultStrategy );
        if ( otherStrategies != null )
        {
            for ( UpstreamDatabaseSelectionStrategy otherStrategy : otherStrategies )
            {
                strategies.push( otherStrategy );
            }
        }
    }

    public MemberId bestUpstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        MemberId result = null;

        for ( UpstreamDatabaseSelectionStrategy strategy : strategies )
        {
            try
            {
                result = strategy.upstreamDatabase().get();
                break;
            }
            catch ( NoSuchElementException ex )
            {
                // Do nothing, this strategy failed
            }
        }

        if ( result == null )
        {
            throw new UpstreamDatabaseSelectionException(
                    "Could not find an upstream database with which to connect." );
        }

        return result;
    }
}
