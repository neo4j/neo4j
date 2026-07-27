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
package org.neo4j.internal.batchimport.input.csv;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.csv.CsvInputIterator.extractHeader;
import static org.neo4j.internal.helpers.Exceptions.throwIfInstanceOfOrUnchecked;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.util.TriConsumer;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.importer.SchemaCommandSource;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.Inputs;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.util.concurrent.Futures;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input {
    private static final String MIXED_COMPOSITE_ID_REFERRALS =
            "How to refer to composite IDs (multiple :ID columns) from :START_ID/:END_ID must be consistent: "
                    + "Either using a single :START_ID/:END_ID column, or a matching amount of :START_ID/:END_ID columns.";
    private static final long ESTIMATE_SAMPLE_SIZE = mebiBytes(1);

    private final Iterable<DataFactory> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final SchemaCommandSource schemaCommandSource;
    private final IdType defaultIdType;
    private final Configuration config;
    private final Monitor monitor;
    private final Groups groups;
    private final Map<Path, Header> headersByPath = new HashMap<>();
    private final boolean autoSkipHeaders;
    private final MemoryTracker memoryTracker;
    private List<Header> cachedNodeHeaders;
    private boolean delimitIds;
    private boolean hasBeenValidated;
    private IdType idType;

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param defaultIdType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param autoSkipHeaders  flag to skip headers
     * @param monitor {@link Monitor} for internal events.
     * @param memoryTracker the {@link MemoryTracker} to use
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory,
            Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory,
            Header.Factory relationshipHeaderFactory,
            IdType defaultIdType,
            Configuration config,
            boolean autoSkipHeaders,
            Monitor monitor,
            MemoryTracker memoryTracker) {
        this(
                nodeDataFactory,
                nodeHeaderFactory,
                relationshipDataFactory,
                relationshipHeaderFactory,
                ResolvedSchemaCommands.of(),
                defaultIdType,
                config,
                autoSkipHeaders,
                monitor,
                new Groups(),
                memoryTracker);
    }

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param defaultIdType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param autoSkipHeaders  flag to skip headers
     * @param monitor {@link Monitor} for internal events.
     * @param groups the ID groups to use
     * @param memoryTracker the {@link MemoryTracker} to use
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory,
            Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory,
            Header.Factory relationshipHeaderFactory,
            IdType defaultIdType,
            Configuration config,
            boolean autoSkipHeaders,
            Monitor monitor,
            Groups groups,
            MemoryTracker memoryTracker) {
        this(
                nodeDataFactory,
                nodeHeaderFactory,
                relationshipDataFactory,
                relationshipHeaderFactory,
                ResolvedSchemaCommands.of(),
                defaultIdType,
                config,
                autoSkipHeaders,
                monitor,
                groups,
                memoryTracker);
    }

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param schemaCommandSource the schema changes to apply to the database after the data is imported.
     * @param defaultIdType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param autoSkipHeaders  flag to skip headers
     * @param monitor {@link Monitor} for internal events.
     * @param groups the ID groups to use
     * @param memoryTracker the {@link MemoryTracker} to use
     *
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory,
            Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory,
            Header.Factory relationshipHeaderFactory,
            SchemaCommandSource schemaCommandSource,
            IdType defaultIdType,
            Configuration config,
            boolean autoSkipHeaders,
            Monitor monitor,
            Groups groups,
            MemoryTracker memoryTracker) {
        this.autoSkipHeaders = autoSkipHeaders;
        this.memoryTracker = memoryTracker;
        assertSaneConfiguration(config);

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.schemaCommandSource = schemaCommandSource;
        this.defaultIdType = defaultIdType;
        this.config = config;
        this.monitor = monitor;
        this.groups = groups;
    }

    private static void assertSaneConfiguration(Configuration config) {
        Map<Character, String> delimiters = new HashMap<>();
        delimiters.put(disallowNewline(config.delimiter(), "delimiter"), "delimiter");
        checkUniqueCharacterAndNotNewline(delimiters, config.arrayDelimiter(), "array delimiter");
        checkUniqueCharacterAndNotNewline(delimiters, config.quotationCharacter(), "quotation character");
        // Vector delimiter may equal array delimiter, so drop array before checking vector.
        delimiters.remove(config.arrayDelimiter());
        checkUniqueCharacterAndNotNewline(delimiters, config.vectorDelimiter(), "vector delimiter");
    }

    private static char disallowNewline(char character, String characterDescription) {
        if (character == '\n' || character == '\r') {
            throw new IllegalArgumentException("A newline character must not be used as the " + characterDescription);
        }
        return character;
    }

    private static void checkUniqueCharacterAndNotNewline(
            Map<Character, String> characters, char character, String characterDescription) {
        disallowNewline(character, characterDescription);
        String conflict = characters.put(character, characterDescription);
        if (conflict != null) {
            throw new IllegalArgumentException("Character '" + character + "' specified by " + characterDescription
                    + " is the same as specified by " + conflict);
        }
    }

    @Override
    public SchemaCommandSource schemaCommandSource() {
        return schemaCommandSource;
    }

    @Override
    public boolean containsVectorData() {
        // CSV cannot prove to have vector data until `validateAndEstimate` has been called.
        // But since that already samples data and throws, there is no point in even capturing if
        // this contains vector data at this point.
        return false;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        Preconditions.checkState(hasBeenValidated, "must call validateAndEstimate before calling nodes");
        return () -> stream(nodeDataFactory, nodeHeaderFactory, badCollector);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        Preconditions.checkState(hasBeenValidated, "must call validateAndEstimate before calling relationships");
        return () -> stream(relationshipDataFactory, relationshipHeaderFactory, badCollector);
    }

    public Map<Path, Header> headersByPath() {
        return Collections.unmodifiableMap(headersByPath);
    }

    public boolean delimitIds() {
        return delimitIds;
    }

    public Configuration config() {
        return config;
    }

    private InputIterator stream(Iterable<DataFactory> data, Header.Factory headerFactory, Collector badCollector) {
        return new CsvGroupInputIterator(
                data.iterator(),
                headerFactory,
                defaultIdType,
                config,
                badCollector,
                groups,
                autoSkipHeaders,
                delimitIds,
                NO_MONITOR);
    }

    @Override
    public IdType idType() {
        Preconditions.checkState(hasBeenValidated, "must call validateAndEstimate before calling idType");
        return idType;
    }

    @Override
    public ReadableGroups groups() {
        return groups;
    }

    @Override
    public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator, int numberOfThreads)
            throws IOException {
        final var seenSourceFiles = new HashSet<String>();
        // parse all node headers and remember all ID spaces
        final MutableBoolean nodesHasAction = new MutableBoolean();
        final MutableBoolean relationshipsHasAction = new MutableBoolean();
        final MutableBoolean hasCompositeIdColumns = new MutableBoolean(false);

        // -1 as a value means that, in different source files,
        // different numbers of IDs have been used for the same group.
        final Map<Group, Integer> numberOfIdsPerGroup = new HashMap<>();
        cachedNodeHeaders = new ArrayList<>();
        final var nodeSample = validateAndEstimate(
                nodeDataFactory,
                nodeHeaderFactory,
                (header, source, noDecorator) -> {
                    cachedNodeHeaders.add(header);
                    if (Arrays.stream(header.entries()).noneMatch(entry -> entry.type() == Type.LABEL) && noDecorator) {
                        monitor.noNodeLabelsSpecified(source);
                    }
                    if (Arrays.stream(header.entries()).anyMatch(e -> e.type() == Type.ACTION)) {
                        nodesHasAction.setTrue();
                    }

                    final var idHeaders = Arrays.stream(header.entries())
                            .filter(e -> e.type() == Type.ID)
                            .toList();
                    final var numIdColumnsGroups = idHeaders.stream()
                            .map(Header.Entry::group)
                            .distinct()
                            .count();
                    Preconditions.checkState(
                            numIdColumnsGroups <= 1,
                            "There are multiple :ID columns, but they are referring to different groups");

                    if (!idHeaders.isEmpty()) {
                        final var numIdColumns = idHeaders.size();
                        if (numIdColumns > 1) {
                            hasCompositeIdColumns.setTrue();
                        }

                        final var group = idHeaders.getFirst().group();
                        if (numberOfIdsPerGroup.getOrDefault(group, numIdColumns) == numIdColumns) {
                            // Either not set yet or already set to numIdColumns
                            numberOfIdsPerGroup.put(group, numIdColumns);
                        } else {
                            // Already set to a different value
                            numberOfIdsPerGroup.put(group, -1);
                        }
                    }
                },
                valueSizeCalculator,
                node -> node.labels().length,
                seenSourceFiles,
                numberOfThreads);

        final MutableBoolean singleStartEndIdColumnRefersToCompositeId = new MutableBoolean(false);
        final MutableBoolean multipleStartEndIdColumnsRefersToCompositeId = new MutableBoolean(false);

        // parse all relationship headers and verify all ID spaces
        final var relationshipSample = validateAndEstimate(
                relationshipDataFactory,
                relationshipHeaderFactory,
                (header, source, noDecorator) -> {
                    // Merely parsing and constructing the header here will as a side effect verify that the id
                    // groups already exists (relationship header isn't allowed to create groups)
                    if (Arrays.stream(header.entries()).noneMatch(entry -> entry.type() == Type.TYPE) && noDecorator) {
                        monitor.noRelationshipTypeSpecified(source);
                    }
                    if (Arrays.stream(header.entries()).anyMatch(e -> e.type() == Type.ACTION)) {
                        relationshipsHasAction.setTrue();
                    }

                    final var startIdHeaders = Arrays.stream(header.entries())
                            .filter(e -> e.type() == Type.START_ID)
                            .toList();
                    final var endIdHeaders = Arrays.stream(header.entries())
                            .filter(e -> e.type() == Type.END_ID)
                            .toList();

                    final var numStartIdColumnsGroups = startIdHeaders.stream()
                            .map(Header.Entry::group)
                            .distinct()
                            .count();
                    Preconditions.checkState(
                            numStartIdColumnsGroups <= 1,
                            "There are multiple :START_ID columns, but they are referring to different groups");

                    final var numEndIdColumnsGroups = endIdHeaders.stream()
                            .map(Header.Entry::group)
                            .distinct()
                            .count();
                    Preconditions.checkState(
                            numEndIdColumnsGroups <= 1,
                            "There are multiple :END_ID columns, but they are referring to different groups");

                    final var startIdGroup = startIdHeaders.getFirst().group();
                    final var endIdGroup = endIdHeaders.getFirst().group();

                    validateStartAndEndIdColumnAmounts(
                            "START_ID",
                            numberOfIdsPerGroup.getOrDefault(startIdGroup, 1),
                            startIdHeaders.size(),
                            startIdGroup,
                            singleStartEndIdColumnRefersToCompositeId,
                            multipleStartEndIdColumnsRefersToCompositeId);
                    validateStartAndEndIdColumnAmounts(
                            "END_ID",
                            numberOfIdsPerGroup.getOrDefault(endIdGroup, 1),
                            endIdHeaders.size(),
                            endIdGroup,
                            singleStartEndIdColumnRefersToCompositeId,
                            multipleStartEndIdColumnsRefersToCompositeId);
                },
                valueSizeCalculator,
                entity -> 0,
                seenSourceFiles,
                numberOfThreads);

        this.delimitIds = hasCompositeIdColumns.isTrue() && singleStartEndIdColumnRefersToCompositeId.isFalse();
        this.idType = autoDetectIdType(defaultIdType, cachedNodeHeaders);
        this.hasBeenValidated = true;

        final var propPreAllocAdditional =
                propertyPreAllocateRounding(nodeSample.propertySize.get() + relationshipSample.propertySize.get()) / 2;
        return Input.knownEstimates(
                nodeSample.entityCount.get(),
                relationshipSample.entityCount.get(),
                nodeSample.propertyCount.get(),
                relationshipSample.propertyCount.get(),
                nodeSample.propertySize.get() + propPreAllocAdditional,
                relationshipSample.propertySize.get() + propPreAllocAdditional,
                nodeSample.labelCount.get(),
                nodesHasAction.isTrue(),
                relationshipsHasAction.isTrue());
    }

    private static void validateStartAndEndIdColumnAmounts(
            String columnName,
            int numberOfIdColumnsForGroup,
            int numIdColumns,
            Group group,
            MutableBoolean singleStartEndIdColumnRefersToCompositeIdColumn,
            MutableBoolean multipleStartEndIdColumnsRefersToCompositeIdColumn) {
        if (numberOfIdColumnsForGroup == 1) {
            Preconditions.checkState(
                    numberOfIdColumnsForGroup == numIdColumns,
                    "There are %d :%s columns for group '%s', but %d :%s columns is expected."
                            .formatted(numIdColumns, columnName, group.name(), numberOfIdColumnsForGroup, columnName));
        } else if (numIdColumns == 1) {
            singleStartEndIdColumnRefersToCompositeIdColumn.setTrue();
            Preconditions.checkState(
                    multipleStartEndIdColumnsRefersToCompositeIdColumn.isFalse(), MIXED_COMPOSITE_ID_REFERRALS);
        } else {
            multipleStartEndIdColumnsRefersToCompositeIdColumn.setTrue();
            final int expectedNumberOfStartIdColumns = numberOfIdColumnsForGroup == -1 ? 1 : numberOfIdColumnsForGroup;
            Preconditions.checkState(
                    expectedNumberOfStartIdColumns == numIdColumns,
                    "There are %d :%s columns for group '%s', but %d :%s columns is expected."
                            .formatted(
                                    numIdColumns,
                                    columnName,
                                    group.name(),
                                    expectedNumberOfStartIdColumns,
                                    columnName));
            Preconditions.checkState(
                    singleStartEndIdColumnRefersToCompositeIdColumn.isFalse(), MIXED_COMPOSITE_ID_REFERRALS);
        }
    }

    private Sample validateAndEstimate(
            Iterable<DataFactory> dataFactories,
            Header.Factory headerFactory,
            TriConsumer<Header, String, Boolean> headerChecker,
            PropertySizeCalculator valueSizeCalculator,
            ToIntFunction<InputEntity> additionalCalculator,
            Set<String> seenSourceFiles,
            int numberOfThreads)
            throws IOException {
        Sample sample = new Sample();
        try (ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads)) {
            final var sampleConfig =
                    config.toBuilder().withReadIsForSampling(true).build();
            int groupId = 0;
            List<Future<Void>> samplings = new ArrayList<>();
            for (var dataFactory : dataFactories) {
                // one input group
                groupId++;
                Header header = null;
                final var data = dataFactory.create(sampleConfig);
                try (var decorator = data.decorator()) {
                    RawIterator<CharReadable, IOException> stream = data.stream();
                    Iterator<Path> originalFiles = Iterators.iterator(dataFactory.files());
                    while (stream.hasNext()) {
                        CharReadable source = stream.next();
                        // source.file() has undergone "adaptation", see Readables.FromFile.adaptPath.
                        // But for the headersByPath Map, we want the original file path.
                        Path originalFile = originalFiles.next();
                        try {
                            final var sourceDescription = source.sourceDescription();
                            if (!seenSourceFiles.add(sourceDescription)) {
                                monitor.duplicateSourceFile(sourceDescription);
                            }

                            if (header == null) {
                                // Parsing and constructing this header will create this group
                                // Extract the header from the first file in this group
                                // This is the only place we monitor type normalization because it's before import and
                                // it touches all headers
                                header = extractHeader(
                                        source, headerFactory, defaultIdType, sampleConfig, groups, monitor);
                                headerChecker.accept(header, sourceDescription, decorator == NO_DECORATOR);
                            }
                            headersByPath.put(originalFile, header);
                        } catch (Throwable t) {
                            source.close();
                            throw t;
                        }

                        int sampleGroupId = groupId;
                        Header sampleHeader = header;
                        samplings.add(executor.submit(() -> {
                            sample(
                                    sampleConfig,
                                    source,
                                    sampleGroupId,
                                    sampleHeader,
                                    decorator,
                                    valueSizeCalculator,
                                    additionalCalculator,
                                    sample);
                            return null;
                        }));
                    }
                }
            }
            try {
                Futures.getAll(samplings);
            } catch (ExecutionException e) {
                // Futures.getAll might make a chain of multiple ExecutionException
                ExecutionException actualExecutionException = e;
                while (actualExecutionException.getCause() instanceof ExecutionException ee) {
                    actualExecutionException = ee;
                }
                throwIfInstanceOfOrUnchecked(
                        actualExecutionException.getCause(), IOException.class, RuntimeException::new);
            }
        }
        return sample;
    }

    /**
     * Whether {@link #validateAndEstimate} reads sample rows to estimate sizes. Header parsing and structural
     * validation run regardless. Test seam: a subclass returning {@code false} validates the input and sets up state
     * without parsing data-row values, so malformed values don't fail estimation.
     */
    @VisibleForTesting
    protected boolean shouldSampleData() {
        return true;
    }

    private void sample(
            Configuration sampleConfig,
            CharReadable source,
            int groupId,
            Header header,
            Decorator decorator,
            PropertySizeCalculator valueSizeCalculator,
            ToIntFunction<InputEntity> additionalCalculator,
            Sample sample)
            throws IOException {
        if (!shouldSampleData()) {
            source.close();
            return;
        }
        // When sampling, we can set delimitIds=false,
        // since there is no checking of duplicates in this visitor.
        try (source;
                var iterator = new CsvInputIterator(
                        source,
                        decorator,
                        header,
                        sampleConfig,
                        defaultIdType,
                        EMPTY,
                        CsvGroupInputIterator.extractors(sampleConfig),
                        groupId,
                        autoSkipHeaders,
                        false);
                var entity = new InputEntity();
                var chunk = new CsvInputChunkProxy()) {
            var entities = 0;
            var properties = 0d;
            var propertySize = 0d;
            var additional = 0d;
            while (iterator.position() < ESTIMATE_SAMPLE_SIZE && iterator.next(chunk)) {
                for (; chunk.next(entity); entities++) {
                    properties += entity.properties.size();
                    propertySize +=
                            Inputs.calculatePropertySize(entity, valueSizeCalculator, NULL_CONTEXT, memoryTracker);
                    additional += additionalCalculator.applyAsInt(entity);
                }
            }
            if (entities > 0) {
                final var position = iterator.position();
                final var actualFileSize = source.length() / iterator.compressionRatio();
                final var entityCountInSource = ((actualFileSize / position) * entities);
                sample.entityCount.addAndGet((long) entityCountInSource);
                sample.propertyCount.addAndGet((long) ((properties / entities) * entityCountInSource));
                sample.propertySize.addAndGet((long) ((propertySize / entities) * entityCountInSource));
                sample.labelCount.addAndGet((long) ((additional / entities) * entityCountInSource));
            }
        }
    }

    @Override
    public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        if (cachedNodeHeaders == null) {
            cachedNodeHeaders = new ArrayList<>();
            try {
                // parse all node headers and remember all ID spaces
                for (DataFactory dataFactory : nodeDataFactory) {
                    Data data = dataFactory.create(config);
                    try (CharSeeker dataStream = charSeeker(new MultiReadable(data.stream()), config, false)) {
                        // Parsing and constructing this header will create this group,
                        // so no need to do something with the result of it right now
                        cachedNodeHeaders.add(DataFactories.defaultFormatNodeFileHeader()
                                .create(dataStream, config, defaultIdType, groups));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        Map<String, SchemaDescriptor> result = new HashMap<>();
        for (Header header : cachedNodeHeaders) {
            collectReferencedNodeSchemaFromHeader(header, tokenHolders, result);
        }
        return result;
    }

    public static void collectReferencedNodeSchemaFromHeader(
            Header header, TokenHolders tokenHolders, Map<String, SchemaDescriptor> result) {
        Arrays.stream(header.entries())
                .filter(e -> e.type() == Type.ID)
                .findAny()
                .ifPresent(entry -> {
                    var options = entry.rawOptions();
                    var labelName = options.get("label");
                    Preconditions.checkState(
                            labelName != null, "No label was specified for the node index in '%s'", entry);
                    var keyName = entry.name();
                    Preconditions.checkState(
                            keyName != null, "No property key was specified for node index in '%s'", entry);
                    var label = tokenHolders.labelTokens().getIdByName(labelName);
                    var key = tokenHolders.propertyKeyTokens().getIdByName(keyName);
                    Preconditions.checkState(
                            label != TokenConstants.NO_TOKEN,
                            "Label '%s' for node index specified in '%s' does not exist",
                            labelName,
                            entry);
                    Preconditions.checkState(
                            key != TokenConstants.NO_TOKEN,
                            "Property key '%s' for node index specified in '%s' does not exist",
                            keyName,
                            entry);
                    var schemaDescriptor = SchemaDescriptors.forLabel(label, key);
                    var prev = result.put(entry.group().name(), schemaDescriptor);
                    Preconditions.checkState(
                            prev == null || prev.equals(schemaDescriptor),
                            "Multiple different indexes for group " + entry.group());
                });
    }

    private static long propertyPreAllocateRounding(long initialEstimatedPropertyStoreSize) {
        if (!IS_OS_LINUX) {
            // Only linux systems does pre-allocation of store files, so the pre-allocation rounding is zero for all
            // other systems.
            return 0;
        }
        // By default, the page cache will grow large store files in 32 MiB sized chunks.
        long preAllocSize = ByteUnit.mebiBytes(32);
        if (initialEstimatedPropertyStoreSize < preAllocSize) {
            return 0;
        }
        long chunks = 1 + initialEstimatedPropertyStoreSize / preAllocSize;
        long estimatedFinalPropertyStoreSize = chunks * preAllocSize;
        // Compute the difference from the initial estimate, to what we anticipate when we account for pre-allocation.
        return estimatedFinalPropertyStoreSize - initialEstimatedPropertyStoreSize;
    }

    public static Extractor<?> idExtractor(IdType idType, Extractors extractors) {
        return switch (idType) {
            case STRING -> extractors.string();
            case INTEGER, ACTUAL -> extractors.long_();
        };
    }

    /**
     * @return the {@link IdType} of the {@code IdMapper} that suits the parsed node headers.
     * Returns {@link IdType#INTEGER} only if every parsed node header contains at most one {@link Type#ID} column
     * and every {@link Type#ID} column produces a value that can be cast to {@code long}.
     * Returns {@link IdType#ACTUAL} when the input is configured with that mode.
     * Falls back to {@code defaultIdType} when no ID column is present at all.
     */
    private static IdType autoDetectIdType(IdType defaultIdType, List<Header> nodeHeaders) {
        if (defaultIdType == IdType.ACTUAL) {
            return IdType.ACTUAL;
        }
        int totalIdColumns = 0;
        for (Header header : nodeHeaders) {
            int idColumnsInHeader = 0;
            for (Header.Entry entry : header.entries()) {
                if (entry.type() != Type.ID) {
                    continue;
                }
                idColumnsInHeader++;
                if (!isLongCastable(entry.extractor())) {
                    return IdType.STRING;
                }
            }
            if (idColumnsInHeader > 1) {
                return IdType.STRING;
            }
            totalIdColumns += idColumnsInHeader;
        }
        return totalIdColumns == 0 ? defaultIdType : IdType.INTEGER;
    }

    private static boolean isLongCastable(Extractor<?> extractor) {
        if (extractor == null) {
            return false;
        }
        var type = extractor.extractedClass();
        return type == long.class || type == int.class || type == short.class || type == byte.class;
    }

    public interface Monitor extends Header.Monitor {
        /**
         * Reports that a given source file has been specified more than one time.
         * @param sourceFile source file that is a duplicate.
         */
        void duplicateSourceFile(String sourceFile);

        /**
         * Reports that a given source file, in this case the first in its group, doesn't specify any node label header and
         * the group wasn't assigned any label from the command line specification.
         * @param sourceFile source file of the first file in the file group that is missing node labels.
         */
        void noNodeLabelsSpecified(String sourceFile);

        /**
         * Reports that a given source file, in this case the first in its group, doesn't specify any relationship type header and
         * the group wasn't assigned a relationship type from the command line specification.
         * @param sourceFile source file of the first file in the file group that is missing the relationship type..
         */
        void noRelationshipTypeSpecified(String sourceFile);
    }

    public static final Monitor NO_MONITOR = new Monitor() {
        @Override
        public void duplicateSourceFile(String sourceFile) { // no-op
        }

        @Override
        public void noNodeLabelsSpecified(String sourceFile) { // no-op
        }

        @Override
        public void noRelationshipTypeSpecified(String sourceFile) { // no-op
        }

        @Override
        public void typeNormalized(String sourceDescription, String header, String fromType, String toType) { // no-op
        }
    };

    public static class PrintingMonitor extends Header.PrintingMonitor implements Monitor {
        private final PrintStream out;

        public PrintingMonitor(PrintStream out) {
            super(out);
            this.out = out;
        }

        @Override
        public void duplicateSourceFile(String sourceFile) {
            out.printf(
                    "WARN: source file %s has been specified multiple times, this may result in unwanted duplicates%n",
                    sourceFile);
        }

        @Override
        public void noNodeLabelsSpecified(String sourceFile) {
            out.printf(
                    "WARN: file group with header file %s specifies no node labels, which could be a mistake%n",
                    sourceFile);
        }

        @Override
        public void noRelationshipTypeSpecified(String sourceFile) {
            out.printf(
                    "WARN: file group with header file %s specifies no relationship type, which could be a mistake%n",
                    sourceFile);
        }
    }

    private record Sample(
            AtomicLong entityCount, AtomicLong propertyCount, AtomicLong propertySize, AtomicLong labelCount) {
        Sample() {
            this(new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong());
        }
    }
}
