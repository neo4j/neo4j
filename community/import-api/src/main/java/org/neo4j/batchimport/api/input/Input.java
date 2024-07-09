/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.batchimport.api.input;

import java.io.IOException;
import java.util.Map;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.token.TokenHolders;

/**
 * Unifies all data input given to a {@link BatchImporter} to allow for more coherent implementations.
 */
public interface Input extends AutoCloseable {
    /**
     * @param numberOfNodes estimated number of nodes for the entire input.
     * @param numberOfRelationships estimated number of relationships for the entire input.
     * @param numberOfNodeProperties estimated number of node properties.
     * @param numberOfRelationshipProperties estimated number of relationship properties.
     * @param sizeOfNodeProperties estimated size that the estimated number of node properties will require on disk.
     * This is a separate estimate since it depends on the type and size of the actual properties.
     * @param sizeOfRelationshipProperties estimated size that the estimated number of relationship properties will require on disk.
     * This is a separate estimate since it depends on the type and size of the actual properties.
     * @param numberOfNodeLabels estimated number of node labels. Examples:
     * <ul>
     * <li>2 nodes, 1 label each ==> 2</li>
     * <li>1 node, 2 labels each ==> 2</li>
     * <li>2 nodes, 2 labels each ==> 4</li>
     * </ul>
     */
    record Estimates(
            long numberOfNodes,
            long numberOfRelationships,
            long numberOfNodeProperties,
            long numberOfRelationshipProperties,
            long sizeOfNodeProperties,
            long sizeOfRelationshipProperties,
            long numberOfNodeLabels) {}

    /**
     * Provides all node data for an import.
     *
     * @param badCollector for collecting bad entries.
     * @return an {@link InputIterator} which will provide all node data for the whole import.
     */
    InputIterable nodes(Collector badCollector);

    /**
     * Provides all relationship data for an import.
     *
     * @param badCollector for collecting bad entries.
     * @return an {@link InputIterator} which will provide all relationship data for the whole import.
     */
    InputIterable relationships(Collector badCollector);

    /**
     * @return {@link IdType} which matches the type of ids this {@link Input} generates.
     * Will get populated by node import and later queried by relationship import
     * to resolve potentially temporary input node ids to actual node ids in the database.
     */
    IdType idType();

    /**
     * @return accessor for id groups that this input has.
     */
    ReadableGroups groups();

    /**
     * @param valueSizeCalculator for calculating property sizes on disk.
     * @return {@link Estimates} for this input w/o reading through it entirely.
     * @throws IOException on I/O error.
     */
    Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) throws IOException;

    /**
     * @return a {@link Map} where key is group name and value which {@link SchemaDescriptor index} it refers to.
     * @param tokenHolders available tokens.
     */
    default Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() {}

    static Input input(
            InputIterable nodes,
            InputIterable relationships,
            IdType idType,
            Estimates estimates,
            ReadableGroups groups) {
        return new Input() {
            @Override
            public InputIterable relationships(Collector badCollector) {
                return relationships;
            }

            @Override
            public InputIterable nodes(Collector badCollector) {
                return nodes;
            }

            @Override
            public IdType idType() {
                return idType;
            }

            @Override
            public ReadableGroups groups() {
                return groups;
            }

            @Override
            public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) {
                return estimates;
            }
        };
    }

    public static Estimates knownEstimates(
            long numberOfNodes,
            long numberOfRelationships,
            long numberOfNodeProperties,
            long numberOfRelationshipProperties,
            long sizeOfNodeProperties,
            long sizeOfRelationshipProperties,
            long numberOfNodeLabels) {
        return new Estimates(
                numberOfNodes,
                numberOfRelationships,
                numberOfNodeProperties,
                numberOfRelationshipProperties,
                sizeOfNodeProperties,
                sizeOfRelationshipProperties,
                numberOfNodeLabels);
    }
}
