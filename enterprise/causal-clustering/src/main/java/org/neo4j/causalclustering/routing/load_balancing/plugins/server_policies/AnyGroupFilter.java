/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;

import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * Only returns servers matching any of the supplied groups.
 */
public class AnyGroupFilter implements Filter<ServerInfo>
{
    private final Predicate<ServerInfo> matchesAnyGroup;
    private final Set<String> groups;

    AnyGroupFilter( String... groups )
    {
        this( asSet( groups ) );
    }

    AnyGroupFilter( Set<String> groups )
    {
        this.matchesAnyGroup = serverInfo ->
        {
            for ( String group : serverInfo.groups() )
            {
                if ( groups.contains( group ) )
                {
                    return true;
                }
            }
            return false;
        };
        this.groups = groups;
    }

    @Override
    public Set<ServerInfo> apply( Set<ServerInfo> data )
    {
        return data.stream().filter( matchesAnyGroup ).collect( Collectors.toSet() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AnyGroupFilter that = (AnyGroupFilter) o;
        return Objects.equals( groups, that.groups );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( groups );
    }

    @Override
    public String toString()
    {
        return "AnyGroupFilter{" +
               "groups=" + groups +
               '}';
    }
}
