/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.api.operations.EntityOperations;
import org.neo4j.kernel.api.operations.LabelOperations;
import org.neo4j.kernel.api.operations.PropertyOperations;
import org.neo4j.kernel.api.operations.SchemaOperations;

/**
 * Interface for accessing and modifying the underlying graph.
 * A statement is executing within a {@link TransactionContext transaction}
 * and will be able to see all previous changes made within that transaction.
 * When done using a statement it must be closed.
 *
 * Note that this interface only combines a set of interfaces that define operations that the
 * database can perform.
 * 
 * One main difference between a {@link TransactionContext} and a {@link StatementContext}
 * is life cycle of some locks, where read locks can live within one statement,
 * whereas write locks will live for the entire transaction.
 */
public interface StatementContext extends
        EntityOperations, PropertyOperations, LabelOperations, SchemaOperations
{
    /**
     * Closes this statement. Statements must be closed when done and before
     * their parent transaction finishes.
     * As an example statement-bound locks can be released when closing
     * a statement. 
     */
    void close();
}
