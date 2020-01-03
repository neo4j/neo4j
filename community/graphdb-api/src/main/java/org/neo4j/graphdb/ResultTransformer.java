/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb;

import java.util.function.Function;

import org.neo4j.annotations.api.PublicApi;

/**
 * Interface used in {@link GraphDatabaseService} for consuming and transforming results of queries that are executed by the database service in
 * separate isolated managed transaction.
 * Implementations should be able to process query results but should be aware that {@link Result} itself
 * and any other transactional entities will not gonna be usable after transaction completion.
 */
@PublicApi
@FunctionalInterface
public interface ResultTransformer<T> extends Function<Result, T>
{
    /**
     * Result consumer that does nothing
     */
    ResultTransformer<Void> EMPTY_TRANSFORMER = result -> null;
}
