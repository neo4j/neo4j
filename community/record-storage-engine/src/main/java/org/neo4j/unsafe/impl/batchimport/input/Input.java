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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.function.ToIntFunction;

import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.NodeImporter;
import org.neo4j.unsafe.impl.batchimport.RelationshipImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.values.storable.Value;

/**
 * Unifies all data input given to a {@link BatchImporter} to allow for more coherent implementations.
 */
public interface Input
{
    interface Estimates
    {
        /**
         * @return estimated number of nodes for the entire input.
         */
        long numberOfNodes();

        /**
         * @return estimated number of relationships for the entire input.
         */
        long numberOfRelationships();

        /**
         * @return estimated number of node properties.
         */
        long numberOfNodeProperties();

        /**
         * @return estimated number of relationship properties.
         */
        long numberOfRelationshipProperties();

        /**
         * @return estimated size that the estimated number of node properties will require on disk.
         * This is a separate estimate since it depends on the type and size of the actual properties.
         */
        long sizeOfNodeProperties();

        /**
         * @return estimated size that the estimated number of relationship properties will require on disk.
         * This is a separate estimate since it depends on the type and size of the actual properties.
         */
        long sizeOfRelationshipProperties();

        /**
         * @return estimated number of node labels. Examples:
         * <ul>
         * <li>2 nodes, 1 label each ==> 2</li>
         * <li>1 node, 2 labels each ==> 2</li>
         * <li>2 nodes, 2 labels each ==> 4</li>
         * </ul>
         */
        long numberOfNodeLabels();
    }

    /**
     * Provides all node data for an import.
     *
     * @return an {@link InputIterator} which will provide all node data for the whole import.
     */
    InputIterable nodes();

    /**
     * Provides all relationship data for an import.
     *
     * @return an {@link InputIterator} which will provide all relationship data for the whole import.
     */
    InputIterable relationships();

    /**
     * @return {@link IdMapper} which will get populated by {@link NodeImporter}
     * and later queried by {@link RelationshipImporter}
     * to resolve potentially temporary input node ids to actual node ids in the database.
     * @param numberArrayFactory The factory for creating data-structures to use for caching internally in the IdMapper.
     */
    IdMapper idMapper( NumberArrayFactory numberArrayFactory );

    /**
     * @return a {@link Collector} capable of writing bad relationships
     * and duplicate nodes to an output stream for later handling.
     */
    Collector badCollector();

    /**
     * @param valueSizeCalculator for calculating property sizes on disk.
     * @return {@link Estimates} for this input w/o reading through it entirely.
     * @throws IOException on I/O error.
     */
    Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator ) throws IOException;
}
