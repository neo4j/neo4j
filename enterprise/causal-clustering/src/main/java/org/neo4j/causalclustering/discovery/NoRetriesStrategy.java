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
package org.neo4j.causalclustering.discovery;

import java.util.function.Function;
import java.util.function.Predicate;

public class NoRetriesStrategy<I, E> implements RetryStrategy<I,E>
{
    @Override
    public E apply( I input, Function<I,E> retriable, Predicate<E> shouldRetry )
    {
        return retriable.apply( input );
    }
}
