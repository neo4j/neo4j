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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

/**
 * Unifies all data input given to a {@link BatchImporter} to allow for more coherent implementations.
 */
public interface Input
{
    /**
     * Provides all {@link InputNode input nodes} for an import. The returned {@link InputIterable iterable's}
     * {@link InputIterable#iterator() iterator()} method may be called multiple times.
     *
     * @return an {@link InputIterable} which will provide all {@link InputNode input nodes} for the whole import.
     */
    InputIterable<InputNode> nodes();

    /**
     * Provides all {@link InputRelationship input relationships} for an import. The returned
     * {@link InputIterable iterable's} {@link InputIterable#iterator() iterator()} method may be called multiple times.
     *
     * @return an {@link InputIterable} which will provide all {@link InputRelationship input relationships}
     * for the whole import.
     */
    InputIterable<InputRelationship> relationships();

    /**
     * @return {@link IdMapper} which will get populated by {@link InputNode#id() input node ids}
     * and later queried by {@link InputRelationship#startNode()} and {@link InputRelationship#endNode()} ids
     * to resolve potentially temporary input node ids to actual node ids in the database.
     */
    IdMapper idMapper();

    /**
     * @return {@link IdGenerator} which is responsible for generating actual node ids from input node ids.
     */
    IdGenerator idGenerator();

    /**
     * @return whether or not {@link InputRelationship input relationships} returned by {@link #relationships()}
     * specify specific actual relationship ids to be used in the database. Either all
     * {@link InputRelationship input relationships} must specify specific ids or none.
     */
    boolean specificRelationshipIds();

    /**
     * @return a {@link Collector} capable of writing {@link InputRelationship bad relationships}
     * and {@link InputNode duplicate nodes} to an output stream for later handling.
     */
    Collector badCollector();
}
