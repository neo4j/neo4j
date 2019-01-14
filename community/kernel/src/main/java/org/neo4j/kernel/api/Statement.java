/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api;

import org.neo4j.graphdb.Resource;

/**
 * A statement which is a smaller coherent unit of work inside a {@link KernelTransaction}.
 *
 * The main purpose of the statement is to keep resources open for the duration of the statement, and
 * then close all resources at statement close.
 *
 * Note that Statement used to be the access-point for all kernel reads and writes before 3.4. For
 * accessing the graph now, see {@link org.neo4j.internal.kernel.api.Transaction}. The only remainder
 * the QueryRegistryOperations, which will eventually also move from here.
 */
public interface Statement extends Resource, ResourceManager
{
    /**
     * @return interface exposing operations for associating metadata with this statement
     */
    QueryRegistryOperations queryRegistration();
}
