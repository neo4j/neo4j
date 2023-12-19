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

/**
 * A strategy pattern for deciding how retries will be handled.
 * <p>
 * Depending on the implementation, it is assumed the retriable function will be executed until conditions satisyfing desired output are met and then the latest
 * (or most valid)
 * result will be returned
 * </p>
 *
 * @param <I> Type of input used for the input function (assumes 1-parameter input functions)
 * @param <E> Type of output returned from retriable function
 */
public interface RetryStrategy<I, E>
{
    /**
     * Run a given function until a satisfying result is achieved
     *
     * @param input the input parameter that is given to the retriable function
     * @param retriable a function that will be executed multiple times until it returns a valid output
     * @param shouldRetry a predicate deciding if the output of the retriable function is valid. Assume that the function will retry if this returns false and
     * exit if it returns true
     * @return the latest (or most valid) output of the retriable function, depending on implementation
     */
    E apply( I input, Function<I,E> retriable, Predicate<E> shouldRetry );
}
