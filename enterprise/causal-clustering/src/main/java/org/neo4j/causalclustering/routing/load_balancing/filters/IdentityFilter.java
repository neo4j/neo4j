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
package org.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.Set;

/**
 * Performs no filtering.
 */
public class IdentityFilter<T> implements Filter<T>
{
    public static final IdentityFilter INSTANCE = new IdentityFilter();

    private IdentityFilter()
    {
    }

    public static <T> IdentityFilter<T> as()
    {
        //noinspection unchecked
        return INSTANCE;
    }

    @Override
    public Set<T> apply( Set<T> data )
    {
        return data;
    }

    @Override
    public String toString()
    {
        return "IdentityFilter{}";
    }
}
