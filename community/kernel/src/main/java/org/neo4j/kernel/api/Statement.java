/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;

public interface Statement extends Resource
{
    ReadOperations readOperations();

    /**
     * We create tokens as part of both schema write transactions and data write transactions.
     * Creating tokens is always allowed, except on read-only databases.
     * Generally we know from context which of these transaction types we are trying to execute, but in Cypher it
     * is harder to distinguish the cases. Therefore this operation set is called out separately.
     */
    TokenWriteOperations tokenWriteOperations();

    DataWriteOperations dataWriteOperations() throws InvalidTransactionTypeKernelException;

    SchemaWriteOperations schemaWriteOperations() throws InvalidTransactionTypeKernelException;
}
