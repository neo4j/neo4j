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

import static java.lang.Integer.min;
import static java.util.Arrays.asList;
import static org.neo4j.internal.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.idExtractor;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.internal.batchimport.input.csv.CsvInput;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.RandomValues;

/**
 * {@link Input} which generates data on the fly. This input wants to know number of nodes and relationships
 * and then a function for generating the nodes and another for generating the relationships.
 * So typical usage would be:
 *
 * <pre>
 * {@code
 * BatchImporter importer = ...
 * Input input = new DataGeneratorInput( 10_000_000, 1_000_000_000,
 *      batch -> {
 *          InputNode[] nodes = new InputNode[batch.getSize()];
 *          for ( int i = 0; i < batch.getSize(); i++ ) {
 *              long id = batch.getStart() + i;
 *              nodes[i] = new InputNode( .... );
 *          }
 *          return nodes;
 *      },
 *      batch -> {
 *          InputRelationship[] relationships = new InputRelationship[batch.getSize()];
 *          ....
 *          return relationships;
 *      } );
 * }
 * </pre>
 */
public class DataGeneratorInput implements Input {
    private final DataDistribution dataDistribution;
    private final IdType idType;
    private final long seed;
    private final RandomValues.Configuration randomConfig;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Groups groups;

    public DataGeneratorInput(
            DataDistribution dataDistribution,
            IdType idType,
            long seed,
            Header nodeHeader,
            Header relationshipHeader,
            Groups groups) {
        this(
                dataDistribution,
                idType,
                seed,
                RandomValues.DEFAULT_CONFIGURATION,
                nodeHeader,
                relationshipHeader,
                groups);
    }

    public DataGeneratorInput(
            DataDistribution dataDistribution,
            IdType idType,
            long seed,
            RandomValues.Configuration randomConfig,
            Header nodeHeader,
            Header relationshipHeader,
            Groups groups) {
        this.dataDistribution = dataDistribution;
        this.idType = idType;
        this.seed = seed;
        this.randomConfig = randomConfig;
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.groups = groups;
    }

    public static DataDistribution data(long nodeCount, long relationshipCount) {
        return new DataDistribution(
                nodeCount,
                relationshipCount,
                new DefaultLabelsGenerator(1, 3),
                new DefaultRelationshipTypeGenerator(1),
                0,
                0,
                0,
                1,
                new DefaultPropertyValueGenerator(20),
                null);
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new RandomEntityDataGenerator(
                dataDistribution, dataDistribution.nodeCount, 10_000, seed, randomConfig, nodeHeader);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new RandomEntityDataGenerator(
                dataDistribution, dataDistribution.relationshipCount, 10_000, seed, randomConfig, relationshipHeader);
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
        int sampleSize = 100;
        InputEntity[] nodeSample = sample(nodes(Collector.EMPTY), sampleSize);
        double labelsPerNodeEstimate = sampleLabels(nodeSample);
        double[] nodePropertyEstimate = sampleProperties(nodeSample, valueSizeCalculator);
        double[] relationshipPropertyEstimate =
                sampleProperties(sample(relationships(Collector.EMPTY), sampleSize), valueSizeCalculator);
        var nodes = dataDistribution.nodeCount;
        var relationships = dataDistribution.relationshipCount;
        return Input.knownEstimates(
                nodes,
                relationships,
                (long) (nodes * nodePropertyEstimate[0]),
                (long) (relationships * relationshipPropertyEstimate[0]),
                (long) (nodes * nodePropertyEstimate[1]),
                (long) (relationships * relationshipPropertyEstimate[1]),
                (long) (nodes * labelsPerNodeEstimate));
    }

    @Override
    public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        Map<String, SchemaDescriptor> schema = new HashMap<>();
        CsvInput.collectReferencedNodeSchemaFromHeader(nodeHeader, tokenHolders, schema);
        return schema;
    }

    private static InputEntity[] sample(InputIterable source, int size) {
        try (InputIterator iterator = source.iterator();
                InputChunk chunk = iterator.newChunk()) {
            InputEntity[] sample = new InputEntity[size];
            int cursor = 0;
            while (cursor < size && iterator.next(chunk)) {
                while (cursor < size && chunk.next(sample[cursor++] = new InputEntity())) {
                    // just loop
                }
            }
            return sample;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static double sampleLabels(InputEntity[] nodes) {
        int labels = 0;
        for (InputEntity node : nodes) {
            if (node != null) {
                labels += node.labels().length;
            }
        }
        return (double) labels / nodes.length;
    }

    private static double[] sampleProperties(InputEntity[] sample, PropertySizeCalculator valueSizeCalculator) {
        if (sample.length == 0 || sample[0] == null) {
            return new double[] {0, 0};
        }

        int propertiesPerEntity = sample[0].propertyCount();
        long propertiesSize = 0;
        for (InputEntity entity : sample) {
            if (entity != null) {
                propertiesSize += Inputs.calculatePropertySize(entity, valueSizeCalculator, NULL_CONTEXT, INSTANCE);
            }
        }
        double propertySizePerEntity = (double) propertiesSize / sample.length;
        return new double[] {propertiesPerEntity, propertySizePerEntity};
    }

    public static Header bareboneNodeHeader(IdType idType, Group group, Extractors extractors) {
        return bareboneNodeHeader(null, idType, group, extractors);
    }

    public static Header bareboneNodeHeader(
            String idKey, IdType idType, Group group, Extractors extractors, Entry... additionalEntries) {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(idKey, Type.ID, group, idExtractor(idType, extractors)));
        entries.add(new Entry(null, Type.LABEL, null, extractors.stringArray()));
        entries.addAll(asList(additionalEntries));
        return new Header(entries.toArray(new Entry[0]));
    }

    public static Header bareboneRelationshipHeader(
            IdType idType, Group group, Extractors extractors, Entry... additionalEntries) {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(null, Type.START_ID, group, idExtractor(idType, extractors)));
        entries.add(new Entry(null, Type.END_ID, group, idExtractor(idType, extractors)));
        entries.add(new Entry(null, Type.TYPE, null, extractors.string()));
        entries.addAll(asList(additionalEntries));
        return new Header(entries.toArray(new Entry[0]));
    }

    public static Header bareboneIncrementalNodeHeader(
            String idKey, String label, IdType idType, Group group, Extractors extractors, Entry... additionalEntries) {
        List<Entry> entries = new ArrayList<>();
        entries.add(
                new Entry(null, idKey, Type.ID, group, idExtractor(idType, extractors), Map.of("label", label), null));
        entries.add(new Entry(null, Type.LABEL, null, extractors.stringArray()));
        entries.addAll(asList(additionalEntries));
        return new Header(entries.toArray(new Entry[0]));
    }

    private static String[] tokens(String prefix, int count) {
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = prefix + (i + 1);
        }
        return result;
    }

    public record DataDistribution(
            long nodeCount,
            long relationshipCount,
            Function<RandomValues, String[]> labelsGenerator,
            Function<RandomValues, String> relationshipTypeGenerator,
            long startNodeId,
            float factorBadNodeData,
            float factorBadRelationshipData,
            float relationshipDistribution,
            BiFunction<Entry, RandomValues, Object> propertyValueGenerator,
            String name) {
        // Basic idea is that there'll be a 1-relationshipDistribution chance that a relationship gets connected to one
        // of the relationshipDistribution nodes
        // 1 meaning dense nodes are evenly spread out across the node ID space. 0 not quite possible.

        public DataDistribution withLabelCount(int labelCount, int maxLabelArrayLength) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    new DefaultLabelsGenerator(labelCount, maxLabelArrayLength),
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withLabelGenerator(Function<RandomValues, String[]> labelsGenerator) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withRelationshipTypeCount(int relationshipTypeCount) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    new DefaultRelationshipTypeGenerator(relationshipTypeCount),
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withRelationshipTypeGenerator(
                Function<RandomValues, String> relationshipTypeGenerator) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withStartNodeId(long startNodeId) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withFactorBadNodeData(float factorBadNodeData) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withFactorBadRelationshipData(float factorBadRelationshipData) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withRelationshipDistribution(float relationshipDistribution) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withMaxStringLength(int maxStringLength) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    new DefaultPropertyValueGenerator(maxStringLength),
                    name);
        }

        public DataDistribution withName(String name) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        public DataDistribution withPropertyValueGenerator(
                BiFunction<Entry, RandomValues, Object> propertyValueGenerator) {
            return new DataDistribution(
                    nodeCount,
                    relationshipCount,
                    labelsGenerator,
                    relationshipTypeGenerator,
                    startNodeId,
                    factorBadNodeData,
                    factorBadRelationshipData,
                    relationshipDistribution,
                    propertyValueGenerator,
                    name);
        }

        @Override
        public String toString() {
            if (name != null) {
                return name;
            }
            return "DataDistribution{" + "nodeCount=" + nodeCount + ", relationshipCount=" + relationshipCount
                    + ", labelsGenerator=" + labelsGenerator + ", relationshipTypeGenerator="
                    + relationshipTypeGenerator + ", startNodeId=" + startNodeId + ", factorBadNodeData="
                    + factorBadNodeData + ", factorBadRelationshipData="
                    + factorBadRelationshipData + ", relationshipDistribution=" + relationshipDistribution
                    + ", propertyValueGenerator="
                    + propertyValueGenerator + '}';
        }
    }

    public static class DefaultLabelsGenerator implements Function<RandomValues, String[]> {
        private final Distribution<String> distribution;
        private final int maxLabelArrayLength;

        public DefaultLabelsGenerator(int labelCount, int maxLabelArrayLength) {
            this("Label", labelCount, maxLabelArrayLength);
        }

        public DefaultLabelsGenerator(String baseName, int labelCount, int maxLabelArrayLength) {
            this.maxLabelArrayLength = maxLabelArrayLength;
            this.distribution = new Distribution<>(tokens(baseName, labelCount));
        }

        @Override
        public String[] apply(RandomValues random) {
            if (distribution.length() == 0) {
                return NO_LABELS;
            }
            int length = random.nextInt(min(maxLabelArrayLength, distribution.length())) + 1;

            String[] result = new String[length];
            for (int i = 0; i < result.length; ) {
                String candidate = distribution.random(random);
                if (!ArrayUtil.contains(result, i, candidate)) {
                    result[i++] = candidate;
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return "DefaultLabelsGenerator{" + distribution.length() + '}';
        }
    }

    public static class DefaultRelationshipTypeGenerator implements Function<RandomValues, String> {
        private final Distribution<String> distribution;

        public DefaultRelationshipTypeGenerator(int relationshipTypeCount) {
            this("TYPE", relationshipTypeCount);
        }

        public DefaultRelationshipTypeGenerator(String baseName, int relationshipTypeCount) {
            this.distribution = new Distribution<>(tokens(baseName, relationshipTypeCount));
        }

        @Override
        public String apply(RandomValues random) {
            return distribution.random(random);
        }

        @Override
        public String toString() {
            return "DefaultRelationshipTypeGenerator{" + distribution.length() + '}';
        }
    }

    public static class DefaultPropertyValueGenerator implements BiFunction<Entry, RandomValues, Object> {
        private final int maxStringLength;
        private final boolean uniqueStrings;
        private final AtomicLong nextId = new AtomicLong();

        public DefaultPropertyValueGenerator(int maxStringLength) {
            this(maxStringLength, false);
        }

        public DefaultPropertyValueGenerator(int maxStringLength, boolean uniqueStrings) {
            this.maxStringLength = maxStringLength;
            this.uniqueStrings = uniqueStrings;
        }

        @Override
        public Object apply(Entry entry, RandomValues random) {
            return switch (entry.extractor().name()) {
                case "String" -> randomString(random);
                case "String[]" -> {
                    int length = random.nextInt(random.intBetween(1, 10));
                    var strings = new String[length];
                    for (int i = 0; i < length; i++) {
                        strings[i] = randomString(random);
                    }
                    yield strings;
                }
                case "boolean" -> random.nextBooleanValue().asObjectCopy();
                case "boolean[]" -> random.nextBooleanArray().asObjectCopy();
                case "byte" -> random.nextByteValue().asObjectCopy();
                case "byte[]" -> random.nextByteArray().asObjectCopy();
                case "short" -> random.nextShortValue().asObjectCopy();
                case "short[]" -> random.nextShortArray().asObjectCopy();
                case "int" -> random.nextIntValue().asObjectCopy();
                case "int[]" -> random.nextIntArray().asObjectCopy();
                case "long" -> random.nextLongValue().asObjectCopy();
                case "long[]" -> random.nextLongArray().asObjectCopy();
                case "float" -> random.nextFloatValue().asObjectCopy();
                case "float[]" -> random.nextFloatArray().asObjectCopy();
                case "double" -> random.nextDoubleValue().asObjectCopy();
                case "double[]" -> random.nextDoubleArray().asObjectCopy();
                default -> throw new IllegalArgumentException(
                        "" + entry + " " + entry.extractor().name());
            };
        }

        private String randomString(RandomValues random) {
            var string = random.nextAlphaNumericTextValue(5, maxStringLength);
            return uniqueStrings ? String.format("%s_%s", string, nextId.getAndIncrement()) : string.stringValue();
        }
    }
}
