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

import static org.neo4j.csv.reader.CharSeekers.charSeeker;

import java.io.IOException;
import java.util.Objects;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.csv.reader.AutoReadingSource;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Chunker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.HeaderSkipper;
import org.neo4j.csv.reader.SectionedCharBuffer;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;

/**
 * {@link Chunker} which parses a chunk of entities when calling {@link #nextChunk(Chunk)},
 * injecting them into {@link EagerCsvInputChunk}, which simply hands them out one by one.
 */
public class EagerParserChunker implements Chunker {
    private final CharSeeker seeker;
    private final CsvInputParser parser;
    private final int chunkSize;
    private final Decorator decorator;

    public EagerParserChunker(
            CharReadable reader,
            IdType idType,
            Header header,
            Collector badCollector,
            Extractors extractors,
            int chunkSize,
            Configuration config,
            Decorator decorator,
            boolean autoSkipHeaders) {
        this.chunkSize = chunkSize;
        this.decorator = decorator;
        this.seeker = charSeeker(
                reader,
                config,
                true,
                (r, c) -> autoSkipHeaders
                        ? new AutoSkipHeaderSource(r, c, idType)
                        : new AutoReadingSource(r, c.bufferSize()));
        this.parser = new CsvInputParser(seeker, config.delimiter(), idType, header, badCollector, extractors);
    }

    @Override
    public boolean nextChunk(Chunk chunk) throws IOException {
        InputEntityArray entities = new InputEntityArray(chunkSize);
        InputEntityVisitor decorated = decorator.apply(entities);
        int cursor = 0;
        for (; cursor < chunkSize && parser.next(decorated); cursor++) { // just loop through and parse
        }

        if (cursor > 0) {
            ((EagerCsvInputChunk) chunk).initialize(entities.toArray());
            return true;
        }
        return false;
    }

    @Override
    public long position() {
        return seeker.position();
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    @Override
    public Chunk newChunk() {
        throw new UnsupportedOperationException();
    }

    private static class AutoSkipHeaderSource implements Source {
        private final CharReadable reader;
        private final HeaderSkipper headerSkipper;
        private SectionedCharBuffer charBuffer;
        private String sourceDescription;

        public AutoSkipHeaderSource(CharReadable reader, Configuration configuration, IdType idType) {
            this.reader = reader;
            this.charBuffer = new SectionedCharBuffer(configuration.bufferSize());
            this.headerSkipper = CsvInputIterator.headerSkip(true, configuration, idType);
        }

        @Override
        public Chunk nextChunk(int seekStartPos) throws IOException {
            charBuffer = reader.read(charBuffer, seekStartPos == -1 ? charBuffer.pivot() : seekStartPos);
            int back = charBuffer.back();
            int length = charBuffer.available();
            int startPosition = charBuffer.pivot();
            var newSourceDescription = reader.sourceDescription();
            if (!Objects.equals(sourceDescription, newSourceDescription)) {
                // We've crossed over to a new file. See if the first line looks like a header.
                int charsSkipped =
                        headerSkipper.skipHeader(charBuffer.array(), charBuffer.back(), charBuffer.available());
                if (charsSkipped > 0) {
                    back += charsSkipped;
                    length -= charsSkipped;
                    startPosition += charsSkipped;
                }
                sourceDescription = newSourceDescription;
            }
            return new GivenChunk(
                    charBuffer.array(), length, charBuffer.pivot(), reader.sourceDescription(), startPosition, back);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
