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
package org.neo4j.internal.batchimport.input;

import java.util.List;
import org.neo4j.internal.batchimport.GeneratingInputIterator;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.RandomsStates;
import org.neo4j.internal.batchimport.input.DataGeneratorInput.DataDistribution;
import org.neo4j.internal.batchimport.input.csv.Deserialization;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

/**
 * Data generator as {@link InputIterator}, parallelizable
 */
public class RandomEntityDataGenerator extends GeneratingInputIterator<RandomValues> {
    public RandomEntityDataGenerator(
            DataDistribution dataDistribution,
            long count,
            int batchSize,
            long seed,
            RandomValues.Configuration randomConfig,
            Header header) {
        super(
                count,
                batchSize,
                new RandomsStates(seed, randomConfig),
                (randoms, visitor, id) -> {
                    for (Entry entry : header.entries()) {
                        switch (entry.type()) {
                            case ID -> {
                                if (dataDistribution.factorBadNodeData() > 0 && id > 0) {
                                    if (randoms.nextFloat() <= dataDistribution.factorBadNodeData()) {
                                        // id between 0 - id
                                        id = randoms.nextLong(id);
                                    }
                                }
                                visitor.id(idValue(entry, id), entry.group());
                                if (entry.name() != null) {
                                    visitor.property(entry.name(), id);
                                }
                            }
                            case PROPERTY -> {
                                Object value = dataDistribution
                                        .propertyValueGenerator()
                                        .apply(entry, randoms);
                                if (value != Values.NO_VALUE && value != null) {
                                    visitor.property(entry.name(), value);
                                }
                            }
                            case LABEL -> visitor.labels(
                                    dataDistribution.labelsGenerator().apply(randoms));
                            case START_ID, END_ID -> {
                                long nodeId =
                                        randoms.nextLong(dataDistribution.nodeCount() + dataDistribution.startNodeId());
                                boolean connectedToDenseNode = dataDistribution.relationshipDistribution() < 1
                                        && randoms.nextFloat() > dataDistribution.relationshipDistribution();
                                if (connectedToDenseNode) {
                                    // Very much poor man's way of distributing relationships such that some nodes
                                    // become more dense than others.
                                    nodeId = (long) ((long) (nodeId * dataDistribution.relationshipDistribution())
                                            / dataDistribution.relationshipDistribution());
                                }
                                if (dataDistribution.factorBadRelationshipData() > 0 && nodeId > 0) {
                                    if (randoms.nextFloat() <= dataDistribution.factorBadRelationshipData()) {
                                        if (randoms.nextBoolean()) {
                                            // simply missing field
                                            break;
                                        }
                                        // referencing some very likely non-existent node id
                                        nodeId = randoms.nextLong() & 0xFFFFFF_FFFFFFFFL;
                                    }
                                }
                                if (entry.type() == Type.START_ID) {
                                    visitor.startId(idValue(entry, nodeId), entry.group());
                                } else {
                                    visitor.endId(idValue(entry, nodeId), entry.group());
                                }
                            }
                            case TYPE -> visitor.type(
                                    dataDistribution.relationshipTypeGenerator().apply(randoms));
                            default -> throw new IllegalArgumentException(entry.toString());
                        }
                    }
                },
                dataDistribution.startNodeId());
    }

    private static Object idValue(Entry entry, long id) {
        return switch (entry.extractor().name()) {
            case "String" -> Long.toString(id);
            case "long" -> id;
            default -> throw new IllegalArgumentException(entry.name());
        };
    }

    /**
     * Test utility method for converting an {@link InputEntity} into another representation.
     *
     * @param entity {@link InputEntity} filled with data.
     * @param deserialization {@link Deserialization}.
     * @param header {@link Header} to deserialize from.
     * @return data from {@link InputEntity} converted into something else.
     */
    public static <T> T convert(InputEntity entity, Deserialization<T> deserialization, Header header) {
        deserialization.clear();
        for (Header.Entry entry : header.entries()) {
            switch (entry.type()) {
                case ID -> deserialization.handle(entry, entity.hasLongId ? entity.longId : entity.objectId);
                case PROPERTY -> deserialization.handle(entry, property(entity.properties, entry.name()));
                case LABEL -> deserialization.handle(entry, entity.labels());
                case TYPE -> deserialization.handle(entry, entity.hasIntType ? entity.intType : entity.stringType);
                case START_ID -> deserialization.handle(
                        entry, entity.hasLongStartId ? entity.longStartId : entity.objectStartId);
                case END_ID -> deserialization.handle(
                        entry, entity.hasLongEndId ? entity.longEndId : entity.objectEndId);
                default -> {} // ignore other types
            }
        }
        return deserialization.materialize();
    }

    private static Object property(List<Object> properties, String key) {
        for (int i = 0; i < properties.size(); i += 2) {
            if (properties.get(i).equals(key)) {
                return properties.get(i + 1);
            }
        }
        return null;
    }
}
