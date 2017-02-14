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
package org.neo4j.causalclustering.load_balancing.strategy.server_policy;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.load_balancing.filters.Filter;

public class AnyTagFilter implements Filter<ServerInfo>
{
    private final Predicate<ServerInfo> matchesAnyTag;

    AnyTagFilter( Set<String> tags )
    {
        this.matchesAnyTag = serverInfo ->
        {
            for ( String tag : serverInfo.tags() )
            {
                if ( tags.contains( tag ) )
                {
                    return true;
                }
            }
            return false;
        };
    }

    @Override
    public Set<ServerInfo> apply( Set<ServerInfo> data )
    {
        return data.stream().filter( matchesAnyTag ).collect( Collectors.toSet() );
    }
}
