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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.neo4j.causalclustering.routing.load_balancing.filters.FilterChain;
import org.neo4j.causalclustering.routing.load_balancing.filters.FirstValidRule;
import org.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;
import org.neo4j.causalclustering.routing.load_balancing.filters.MinimumCountFilter;

class FilterBuilder
{
    private List<Filter<ServerInfo>> current = new ArrayList<>();
    private List<FilterChain<ServerInfo>> rules = new ArrayList<>();

    static FilterBuilder filter()
    {
        return new FilterBuilder();
    }

    FilterBuilder min( int minCount )
    {
        current.add( new MinimumCountFilter<>( minCount ) );
        return this;
    }

    FilterBuilder groups( String... groups )
    {
        current.add( new AnyGroupFilter( groups ) );
        return this;
    }

    FilterBuilder all()
    {
        current.add( IdentityFilter.as() );
        return this;
    }

    FilterBuilder newRule()
    {
        if ( !current.isEmpty() )
        {
            rules.add( new FilterChain<>( current ) );
            current = new ArrayList<>();
        }
        return this;
    }

    Filter<ServerInfo> build()
    {
        newRule();
        return new FirstValidRule<>( rules );
    }
}
