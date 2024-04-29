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

import static java.util.Arrays.copyOf;
import static org.neo4j.csv.reader.Readables.extractFirstLineFrom;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.parseHeaderEntries;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Supplier;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Chunker;
import org.neo4j.csv.reader.ClosestNewLineChunker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.HeaderSkipper;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.csv.Header.Monitor;

/**
 * Iterates over one stream of data, where all data items conform to the same {@link Header}.
 * Typically created from {@link CsvGroupInputIterator}.
 */
class CsvInputIterator implements SourceTraceability, Closeable {

    private static final ImmutableMap<String, Type> TYPES =
            Lists.immutable.of(Type.values()).toMap(Enum::name, t -> t).toImmutable();

    private final CharReadable stream;
    private final Chunker chunker;
    private final int groupId;
    private final Decorator decorator;
    private final Supplier<CsvInputChunk> realInputChunkSupplier;

    CsvInputIterator(
            CharReadable stream,
            Decorator decorator,
            Header header,
            Configuration config,
            IdType idType,
            Collector badCollector,
            Extractors extractors,
            int groupId,
            boolean autoSkipHeaders) {
        this.stream = stream;
        this.decorator = decorator;
        this.groupId = groupId;
        if (config.multilineFields()) {
            // If we're expecting multi-line fields then there's no way to arbitrarily chunk the underlying data source
            // and find record delimiters with certainty. This is why we opt for a chunker that does parsing inside
            // the call that normally just hands out an arbitrary amount of characters to parse outside and in parallel.
            // This chunker is single-threaded, as it was previously too and keeps the functionality of multi-line
            // fields.
            this.chunker = new EagerParserChunker(
                    stream, idType, header, badCollector, extractors, 1_000, config, decorator, autoSkipHeaders);
            this.realInputChunkSupplier = EagerCsvInputChunk::new;
        } else {
            this.chunker =
                    new ClosestNewLineChunker(stream, config.bufferSize(), headerSkip(autoSkipHeaders, config, idType));
            this.realInputChunkSupplier = () -> new LazyCsvInputChunk(
                    idType,
                    config.delimiter(),
                    badCollector,
                    CsvGroupInputIterator.extractors(config),
                    chunker.newChunk(),
                    config,
                    decorator,
                    header);
        }
    }

    CsvInputIterator(
            CharReadable stream,
            Decorator decorator,
            Header.Factory headerFactory,
            IdType idType,
            Configuration config,
            Groups groups,
            Collector badCollector,
            Extractors extractors,
            int groupId,
            boolean autoSkipHeader,
            Monitor monitor)
            throws IOException {
        this(
                stream,
                decorator,
                extractHeader(stream, headerFactory, idType, config, groups, monitor),
                config,
                idType,
                badCollector,
                extractors,
                groupId,
                autoSkipHeader);
    }

    static Header extractHeader(
            CharReadable stream,
            Header.Factory headerFactory,
            IdType idType,
            Configuration config,
            Groups groups,
            Monitor monitor)
            throws IOException {
        if (!headerFactory.isDefined()) {
            CharSeeker headerSeeker = seeker(stream.sourceDescription(), config, extractFirstLineFrom(stream));
            return headerFactory.create(headerSeeker, config, idType, groups, monitor);
        }

        return headerFactory.create(null, null, null, null, monitor);
    }

    private static CharSeeker seeker(String sourceDescription, Configuration config, char[] data) {
        // make the chunk slightly bigger than the header to not have the seeker think that it's reading
        // a value bigger than its max buffer size
        ChunkImpl firstChunk = new ChunkImpl(copyOf(data, data.length + 1));
        firstChunk.initialize(0, data.length, sourceDescription);
        return seeker(firstChunk, config);
    }

    static HeaderSkipper headerSkip(boolean autoSkipHeaders, Configuration config, IdType idType) {
        if (!autoSkipHeaders) {
            return HeaderSkipper.NO_SKIP;
        }

        return (data, offset, length) -> {
            int initialEolSkipped = 0;
            while (BufferedCharSeeker.isEolChar(data[offset])) {
                offset++;
                initialEolSkipped++;
            }

            char[] firstLine = extractFirstLineFrom(data, offset, length);
            if (firstLine.length > 0) {
                CharSeeker seeker = seeker("", config, firstLine);
                try {
                    Header.Entry[] entries = parseHeaderEntries(
                            seeker,
                            config,
                            idType,
                            new Groups(),
                            ZoneId::systemDefault,
                            (sourceDescription, entryIndex, spec, extractors, idExtractor, groups, monitor) ->
                                    new Header.Entry(
                                            spec.rawEntry(),
                                            spec.name(),
                                            typeFromSpec(spec.type()),
                                            null,
                                            extractors.string()),
                            Header.NO_MONITOR);
                    // OK were able to parse this line as a header, skip it
                    if (Arrays.stream(entries).anyMatch(e -> e.type() != Type.PROPERTY && e.type() != Type.IGNORE)) {
                        // This line really looks like a header line
                        return initialEolSkipped + firstLine.length;
                    }
                } catch (Exception e) {
                    // This line didn't look like a header, keep it as a data line
                }
            }
            return 0;
        };
    }

    private static Type typeFromSpec(String specType) {
        var type = Type.PROPERTY;
        if (specType != null) {
            // catch the case when the header spec has come from something like `field_name:int`
            // the specType in this case would be `int` which isn't a valid Type
            type = TYPES.getIfAbsentValue(specType.toUpperCase(Locale.ROOT), type);
        }

        return type;
    }

    public boolean next(CsvInputChunkProxy proxy) throws IOException {
        proxy.ensureInstantiated(realInputChunkSupplier, groupId);
        return proxy.fillFrom(chunker);
    }

    @Override
    public void close() throws IOException {
        chunker.close();
        decorator.close();
    }

    @Override
    public String sourceDescription() {
        return stream.sourceDescription();
    }

    @Override
    public long position() {
        return chunker.position();
    }

    @Override
    public float compressionRatio() {
        return stream.compressionRatio();
    }

    static CharSeeker seeker(Chunk chunk, Configuration config) {
        return new BufferedCharSeeker(Source.singleChunk(chunk), config);
    }
}
