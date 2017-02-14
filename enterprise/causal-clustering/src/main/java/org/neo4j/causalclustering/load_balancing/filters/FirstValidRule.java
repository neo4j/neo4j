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
package org.neo4j.causalclustering.load_balancing.filters;

import java.util.List;
import java.util.Set;

/**
 * Each chain of filters is considered a rule and they are evaluated in order. The result
 * of the first rule to return a valid result (non-empty set) will be the final result.
 */
public class FirstValidRule<T> implements Filter<T>
{
    private List<FilterChain<T>> chains;

    public FirstValidRule( List<FilterChain<T>> chains )
    {
        this.chains = chains;
    }

    @Override
    public Set<T> apply( Set<T> input )
    {
        Set<T> output = input;
        for ( Filter<T> chain : chains )
        {
            output = chain.apply( input );
            if ( !output.isEmpty() )
            {
                break;
            }
        }
        return output;
    }
}
