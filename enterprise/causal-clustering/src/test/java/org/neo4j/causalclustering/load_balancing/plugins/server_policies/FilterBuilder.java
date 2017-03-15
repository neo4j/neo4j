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
package org.neo4j.causalclustering.load_balancing.plugins.server_policies;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.causalclustering.load_balancing.filters.FilterChain;
import org.neo4j.causalclustering.load_balancing.filters.FirstValidRule;
import org.neo4j.causalclustering.load_balancing.filters.IdentityFilter;
import org.neo4j.causalclustering.load_balancing.filters.MinimumCountFilter;

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
