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
package org.neo4j.kernel.api.database.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.BufferBackedChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

@ExtendWith(RandomExtension.class)
class ValuesReadWriteTest {

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource(ValueType.class)
    void valueRoundTrips(ValueType type) throws IOException {
        doRoundTrips(random.randomValues().nextValueOfType(type));
    }

    @ParameterizedTest
    @EnumSource(RandomVirtualValue.class)
    void virtualValueRoundTrips(RandomVirtualValue type) throws IOException {
        doRoundTrips(type.create(random));
    }

    private void doRoundTrips(AnyValue value) throws IOException {
        final var positions = new int[random.nextInt(50, 666)];
        try (var writeChannel = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE)) {
            final var writer = new ValuesWriter(writeChannel);
            for (var i = 0; i < positions.length; i++) {
                positions[i] = writer.write(value);
            }

            var buffer = fill(writeChannel.flip());
            for (var i = 0; i < positions.length; i++) {
                assertThat(ValuesReader.BY_ID.get(buffer.get()).read(buffer)).isEqualTo(value);
            }

            for (var position : positions) {
                final var type = buffer.position(position).get();
                assertThat(ValuesReader.BY_ID.get(type).read(buffer)).isEqualTo(value);
            }
        }
    }

    private enum RandomVirtualValue {
        NODE {
            @Override
            NodeValue create(RandomSupport random) {
                return VirtualValues.nodeValue(
                        random.nextLong(0, Long.MAX_VALUE),
                        random.nextString(),
                        random.randomValues().nextTextArray(),
                        map(random, false),
                        random.nextBoolean());
            }
        },
        NODE_REFERENCE {
            @Override
            VirtualNodeValue create(RandomSupport random) {
                return VirtualValues.node(random.nextLong(0, Long.MAX_VALUE));
            }
        },
        RELATIONSHIP {
            @Override
            RelationshipValue create(RandomSupport random) {
                return VirtualValues.relationshipValue(
                        random.nextLong(0, Long.MAX_VALUE),
                        random.nextString(),
                        VirtualValues.node(random.nextLong(0, Long.MAX_VALUE), random.nextString()),
                        VirtualValues.node(random.nextLong(0, Long.MAX_VALUE), random.nextString()),
                        random.randomValues().nextTextValue(),
                        map(random, false),
                        random.nextBoolean());
            }
        },
        RELATIONSHIP_REFERENCE {
            @Override
            VirtualRelationshipValue create(RandomSupport random) {
                return VirtualValues.relationship(random.nextLong(0, Long.MAX_VALUE));
            }
        },
        PATH {
            @Override
            PathValue create(RandomSupport random) {
                final var nodes = new NodeValue[random.nextInt(2, 13)];
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = (NodeValue) NODE.create(random);
                }

                final var relationships = new RelationshipValue[nodes.length - 1];
                for (int i = 0; i < relationships.length; i++) {
                    relationships[i] = (RelationshipValue) RELATIONSHIP.create(random);
                }

                return VirtualValues.path(nodes, relationships);
            }
        },
        PATH_ID_REFERENCE {
            @Override
            VirtualPathValue create(RandomSupport random) {
                final var nodes = new long[random.nextInt(2, 13)];
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = random.nextLong(0, Long.MAX_VALUE);
                }

                final var relationships = new long[nodes.length - 1];
                for (int i = 0; i < relationships.length; i++) {
                    relationships[i] = random.nextLong(0, Long.MAX_VALUE);
                }

                return VirtualValues.pathReference(nodes, relationships);
            }
        },
        PATH_VALUE_REFERENCE {
            @Override
            VirtualPathValue create(RandomSupport random) {
                final var nodes = new VirtualNodeValue[random.nextInt(2, 13)];
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = VirtualValues.node(random.nextLong(0, Long.MAX_VALUE));
                }

                final var relationships = new VirtualRelationshipValue[nodes.length - 1];
                for (int i = 0; i < relationships.length; i++) {
                    relationships[i] = VirtualValues.relationship(random.nextLong(0, Long.MAX_VALUE));
                }

                return VirtualValues.pathReference(Arrays.asList(nodes), Arrays.asList(relationships));
            }
        },
        LIST {
            @Override
            ListValue create(RandomSupport random) {
                final var values = new AnyValue[random.nextInt(0, 13)];
                for (int i = 0; i < values.length; i++) {
                    values[i] = value(random, true);
                }
                return VirtualValues.list(values);
            }
        },
        MAP {
            @Override
            MapValue create(RandomSupport random) {
                return map(random, true);
            }
        };

        abstract AnyValue create(RandomSupport random);

        private static final List<RandomVirtualValue> VALUES = List.of(values());

        static MapValue map(RandomSupport random, boolean allowNesting) {
            final var entryCount = random.nextInt(0, 13);
            final var keys = new String[entryCount];
            final var values = new AnyValue[entryCount];
            for (int i = 0; i < entryCount; i++) {
                keys[i] = random.nextString();
                values[i] = value(random, allowNesting);
            }
            return VirtualValues.map(keys, values);
        }

        static AnyValue value(RandomSupport random, boolean allowNesting) {
            if (!allowNesting || random.nextInt(0, 100) < 90) {
                return random.nextValue();
            } else {
                return random.among(VALUES).create(random);
            }
        }
    }

    private static ByteBuffer fill(WriteEnrichmentChannel channel) throws IOException {
        final var buffer = ByteBuffer.allocate(channel.size()).order(ByteOrder.LITTLE_ENDIAN);
        try (var bufferChannel = new BufferBackedChannel(buffer)) {
            channel.serialize(bufferChannel);
            return buffer.flip();
        }
    }
}
