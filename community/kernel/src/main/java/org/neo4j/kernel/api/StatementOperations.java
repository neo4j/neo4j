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
import org.neo4j.kernel.api.operations.KeyOperations;
import org.neo4j.kernel.api.operations.ReadOperations;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.api.operations.WriteOperations;

/**
 * Interface for accessing and modifying the underlying graph.
 * A statement is executing within a {@link KernelTransaction transaction}
 * and will be able to see all previous changes made within that transaction.
 * When done using a statement it must be closed.
 *
 * Note that this interface only combines a set of interfaces that define operations that the
 * database can perform.
 *
 * One main difference between a {@link KernelTransaction} and a {@link StatementOperations}
 * is life cycle of some locks, where read locks can live within one statement,
 * whereas write locks will live for the entire transaction.
 *
 * === Method Names ===
 *
 * The most prominent entity the method deals with is the first word of the method name.
 * If dealt with in general, the word is pluralized.
 * We prefer physical entities to meta-entities (e.g. node is preferred to index and label).
 * {@link #nodesGetFromIndexLookup(org.neo4j.kernel.impl.api.index.IndexDescriptor, Object)} is a good example for both
 * of the last two rules, nodes are the physical entities we are interested in, therefore that word is first, the
 * index is just a means of getting to the nodes.
 *
 * Parameter order for all methods, should reflect the order of words in the method name, with the most prominent entity
 * being the first parameter.
 * 
 * This interface should not be implemented directly by classes providing just some parts of it all. Instead
 * implement the specific sub-interfaces.
 */
public interface StatementOperations
        extends ReadOperations, WriteOperations,
                KeyOperations, EntityOperations, SchemaOperations
{
}
