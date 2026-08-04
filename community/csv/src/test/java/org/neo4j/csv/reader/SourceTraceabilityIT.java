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
package org.neo4j.csv.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.csv.reader.HeaderSkipper.NO_SKIP;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.EntityType;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class SourceTraceabilityIT {
    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @MethodSource("permutations")
    void shouldProvideCorrectLineNumbersForConcurrentChunkedReading(boolean withNewlineText, boolean multiLineChunker)
            throws IOException {
        // given
        Map<String, Map<Long, Pair<Integer, String>>> dataMap = new ConcurrentHashMap<>();
        var data = simpleData(withNewlineText, dataMap);
        int bufferSize = (int) kibiBytes(1);
        var configuration = Configuration.newBuilder()
                .withBufferSize(bufferSize)
                .withMultilineDocuments(alwaysTrue())
                .build();
        var extractors = new Extractors(configuration.arrayDelimiter(), configuration.vectorDelimiter());
        try (var chunker = multiLineChunker
                ? new MultiLineChunker(data, configuration, NO_SKIP)
                : new ClosestNewLineChunker(data, bufferSize, NO_SKIP)) {
            // when
            Race race = new Race();
            race.addContestants(
                    4,
                    Race.throwing(() -> {
                        var chunk = chunker.newChunk();
                        while (chunker.nextChunk(chunk)) {
                            try (var seeker =
                                    new BufferedCharSeeker(Source.singleChunk(chunk), configuration, EntityType.NODE)) {
                                var mark = new Mark();
                                while (true) {
                                    // Here we know that the data format is <lineNumber>,<i>,<some text>
                                    String sourceDescription =
                                            extractNext(seeker, mark, configuration, extractors.string());
                                    if (sourceDescription == null) {
                                        // We reached the end of the data chunk
                                        break;
                                    }
                                    String seekerSourceDescription = seeker.sourceDescription();
                                    long seekerLineNumber = seeker.lineNumber();
                                    Long lineNumber = extractNext(seeker, mark, configuration, extractors.long_());
                                    Integer i = extractNext(seeker, mark, configuration, extractors.int_());
                                    String text = extractNext(seeker, mark, configuration, extractors.string());
                                    var expected =
                                            dataMap.get(sourceDescription).remove(lineNumber);
                                    assertThat(expected).isEqualTo(Pair.of(i, text));
                                    assertThat(seekerLineNumber).isEqualTo(lineNumber);
                                    assertThat(seekerSourceDescription).isEqualTo(sourceDescription);
                                }
                            }
                        }
                    }),
                    1);
            race.goUnchecked();

            // then
            assertThat(dataMap.values()).allMatch(Map::isEmpty);
        }
    }

    private static Stream<Arguments> permutations() {
        return Stream.of(Arguments.of(false, false), Arguments.of(false, true), Arguments.of(true, true));
    }

    private static <T> T extractNext(
            BufferedCharSeeker seeker, Mark mark, Configuration configuration, Extractor<T> extractor)
            throws IOException {
        if (!seeker.seek(mark, configuration.delimiter())) {
            return null;
        }
        return seeker.extract(mark, extractor);
    }

    private CharReadable simpleData(boolean withNewlineText, Map<String, Map<Long, Pair<Integer, String>>> dataMap) {
        int numSources = random.nextInt(2, 5);
        List<CharReadable> sources = new ArrayList<>(numSources);
        for (int s = 0; s < numSources; s++) {
            var sourceDescription = "source-" + s;
            Map<Long, Pair<Integer, String>> sourceDataMap = new ConcurrentHashMap<>();
            dataMap.put(sourceDescription, sourceDataMap);
            var data = new StringBuilder();
            for (int i = 0, lineNumber = 1; i < 10_000; i++, lineNumber++) {
                var text = new StringBuilder(randomText());
                int numAdditionalLines = 0;
                if (withNewlineText && random.nextFloat() < 0.1) {
                    numAdditionalLines = random.nextInt(1, 3);
                    for (int a = 0; a < numAdditionalLines; a++) {
                        text.append(String.format("%n%s", randomText()));
                    }
                }
                data.append(String.format("%s,%d,%d,\"%s\"%n", sourceDescription, lineNumber, i, text));
                sourceDataMap.put((long) lineNumber, Pair.of(i, text.toString()));
                lineNumber += numAdditionalLines;
            }
            sources.add(Readables.wrap(sourceDescription, data.toString()));
        }
        var sourcesIterator = sources.iterator();
        return new MultiReadable(new RawIterator<>() {
            @Override
            public boolean hasNext() {
                return sourcesIterator.hasNext();
            }

            @Override
            public CharReadable next() {
                return sourcesIterator.next();
            }
        });
    }

    private String randomText() {
        return random.nextAlphaNumericString();
    }
}
