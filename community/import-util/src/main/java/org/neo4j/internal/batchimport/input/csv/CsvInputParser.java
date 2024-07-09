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

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.UnexpectedEndOfInputException;

/**
 * CSV data to input entity parsing logic. Parsed CSV data is fed into {@link InputEntityVisitor}.
 */
public class CsvInputParser implements Closeable {
    private final CharSeeker seeker;
    private final Mark mark = new Mark();
    private final IdType idType;
    private final Header header;
    private final int delimiter;
    private final Collector badCollector;
    private final Extractor<String> stringExtractor;
    private final IdValueBuilder idValueBuilder = new IdValueBuilder();

    private long lineNumber;

    public CsvInputParser(
            CharSeeker seeker,
            int delimiter,
            IdType idType,
            Header header,
            Collector badCollector,
            Extractors extractors) {
        this.seeker = seeker;
        this.delimiter = delimiter;
        this.idType = idType;
        this.header = header;
        this.badCollector = badCollector;
        this.stringExtractor = extractors.string();
    }

    boolean next(InputEntityVisitor visitor) throws IOException {
        lineNumber++;
        int i = 0;
        Header.Entry entry = null;
        Header.Entry[] entries = header.entries();
        try {
            boolean doContinue = true;
            idValueBuilder.clear();
            for (i = 0; i < entries.length && doContinue; i++) {
                entry = entries[i];
                if (!seeker.seek(mark, delimiter)) {
                    if (i > 0) {
                        throw new UnexpectedEndOfInputException("Near " + mark);
                    }
                    // We're just at the end
                    return false;
                }

                if (entry.type() == Type.IGNORE) {
                    continue;
                }

                var extractor = entry.extractor();
                Object value = seeker.tryExtract(mark, extractor, entry.optionalParameter());
                if (extractor.isEmpty(value)) {
                    continue;
                }

                doContinue = switch (entry.type()) {
                    case ID -> switch (idType) {
                        case STRING, INTEGER -> {
                            idValueBuilder.part(value, entry);
                            yield true;
                        }
                        case ACTUAL -> visitor.id((Long) value);
                    };
                    case START_ID -> switch (idType) {
                        case STRING, INTEGER -> visitor.startId(value, entry.group());
                        case ACTUAL -> visitor.startId((Long) value);
                    };
                    case END_ID -> switch (idType) {
                        case STRING, INTEGER -> visitor.endId(value, entry.group());
                        case ACTUAL -> visitor.endId((Long) value);
                    };
                    case TYPE -> visitor.type((String) value);
                    case PROPERTY -> !isEmptyArray(value) && visitor.property(entry.name(), value);
                    case LABEL -> value.getClass().isArray()
                            ? visitor.labels((String[]) value)
                            : visitor.labels(new String[] {(String) value});
                    default -> throw new IllegalArgumentException(entry.type().toString());
                };

                if (mark.isEndOfLine()) {
                    // We're at the end of the line, break and return an entity with what we have.
                    break;
                }
            }

            // Feed the aggregated :ID columns data after all columns have been processed
            if (!idValueBuilder.isEmpty()) {
                doContinue = visitor.id(idValueBuilder.value(), idValueBuilder.group());
                if (doContinue) {
                    for (var idPropertyValue : idValueBuilder.idPropertyValues()) {
                        doContinue = visitor.property(idPropertyValue.name(), idPropertyValue.value());
                    }
                }
            }

            while (!mark.isEndOfLine()) {
                seeker.seek(mark, delimiter);
                if (doContinue) {
                    var value = seeker.tryExtract(mark, stringExtractor, entry.optionalParameter());
                    badCollector.collectExtraColumns(seeker.sourceDescription(), lineNumber, value);
                }
            }
            visitor.endOfEntity();
            return true;
        } catch (final RuntimeException e) {
            String stringValue = null;
            try {
                Extractors extractors = new Extractors('?');
                stringValue = seeker.tryExtract(mark, extractors.string(), entry.optionalParameter());
            } catch (Exception e1) { // OK
            }

            String message = format(
                    "ERROR in input" + "%n  data source: %s"
                            + "%n  in field: %s"
                            + "%n  for header: %s"
                            + "%n  raw field value: %s"
                            + "%n  original error: %s",
                    seeker, entry + ":" + (i + 1), header, stringValue != null ? stringValue : "??", e.getMessage());

            throw new InputException(message, e);
        }
    }

    private static boolean isEmptyArray(Object value) {
        return value.getClass().isArray() && Array.getLength(value) == 0;
    }

    @Override
    public void close() throws IOException {
        seeker.close();
    }
}
