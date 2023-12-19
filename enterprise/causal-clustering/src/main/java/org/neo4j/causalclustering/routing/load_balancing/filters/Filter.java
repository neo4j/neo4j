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
 * A filter for sets.
 *
 * A convention used for filters is to return an empty set if the result is to
 * be interpreted as invalid. This is used for example in rule-lists where the first
 * rule to return a valid non-empty result will be used.
 */
@FunctionalInterface
public interface Filter<T>
{
    Set<T> apply( Set<T> data );
}
