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


import java.util.LinkedHashSet;
import java.util.NoSuchElementException;

import org.neo4j.causalclustering.identity.MemberId;

import static org.neo4j.helpers.collection.Iterables.count;

public class UpstreamDatabaseStrategySelector
{
    private LinkedHashSet<UpstreamDatabaseSelectionStrategy> strategies = new LinkedHashSet<>();
    private MemberId myself;

    UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        this( defaultStrategy, null, null );
    }

    UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy,
            Iterable<UpstreamDatabaseSelectionStrategy> otherStrategies, MemberId myself )
    {
        this.myself = myself;
        System.out.println( "No of additional strategies --> " + count( otherStrategies ) );
        if ( otherStrategies != null )
        {
            for ( UpstreamDatabaseSelectionStrategy otherStrategy : otherStrategies )
            {
                strategies.add( otherStrategy );
            }
        }
        strategies.add( defaultStrategy );

        System.out.println( "Loaded strategies --> " );
        for ( UpstreamDatabaseSelectionStrategy strategy : strategies )
        {
            System.out.println( strategy.getClass() );
        }
    }

    public MemberId bestUpstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        MemberId result = null;
        System.out.println("--------------------------------------------------------------------------------");
        for ( UpstreamDatabaseSelectionStrategy strategy : this.strategies )
        {
            try
            {
                System.out.println( "Trying " + myself + " --> " + strategy.getClass() );
                result = strategy.upstreamDatabase().get();
                break;
            }
            catch ( NoSuchElementException ex )
            {
                // Do nothing, this strategy failed
            }
        }
        System.out.println("--------------------------------------------------------------------------------");

        if ( result == null )
        {
            throw new UpstreamDatabaseSelectionException(
                    "Could not find an upstream database with which to connect." );
        }

        return result;
    }
}
