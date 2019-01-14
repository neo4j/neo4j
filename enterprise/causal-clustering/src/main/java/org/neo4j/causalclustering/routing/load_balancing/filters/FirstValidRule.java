/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Each chain of filters is considered a rule and they are evaluated in order. The result
 * of the first rule to return a valid result (non-empty set) will be the final result.
 */
public class FirstValidRule<T> implements Filter<T>
{
    private List<FilterChain<T>> rules;

    public FirstValidRule( List<FilterChain<T>> rules )
    {
        this.rules = rules;
    }

    @Override
    public Set<T> apply( Set<T> input )
    {
        Set<T> output = input;
        for ( Filter<T> chain : rules )
        {
            output = chain.apply( input );
            if ( !output.isEmpty() )
            {
                break;
            }
        }
        return output;
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
        FirstValidRule<?> that = (FirstValidRule<?>) o;
        return Objects.equals( rules, that.rules );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( rules );
    }

    @Override
    public String toString()
    {
        return "FirstValidRule{" +
               "rules=" + rules +
               '}';
    }
}
