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
package org.neo4j.internal.schema;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.ArrayUtil.concat;
import static org.neo4j.internal.schema.IndexSettingTestUtils.settings;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingExtractors.BooleanExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.DoubleExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.IntegerExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.StringExtractor;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;

class IndexSettingExtractorsTest {
    private static final IndexSettingExtractor[] INDIVIDUAL_EXTRACTORS = new IndexSettingExtractor[] {
        BooleanExtractor.of(TestIndexSetting.BOOLEAN),
        IntegerExtractor.of(TestIndexSetting.INTEGER),
        DoubleExtractor.of(TestIndexSetting.DOUBLE),
        StringExtractor.of(TestIndexSetting.STRING)
    };

    private static final IndexSettingExtractors EXTRACTORS = new IndexSettingExtractors(INDIVIDUAL_EXTRACTORS);

    @Test
    void correctSettings() {
        Set<IndexSetting> settings = new HashSet<>(INDIVIDUAL_EXTRACTORS.length);
        for (IndexSettingExtractor extractor : INDIVIDUAL_EXTRACTORS) {
            settings.add(extractor.setting());
        }

        assertThat(EXTRACTORS.settings()).hasSameElementsAs(settings);
    }

    @ParameterizedTest
    @MethodSource
    void requireOnlyOneExtractorPerSetting(IndexSettingExtractor[] extractors) {
        assertThatThrownBy(() -> new IndexSettingExtractors(extractors))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Expected a single",
                        IndexSettingExtractor.class.getSimpleName(),
                        "to be provided for each",
                        IndexSetting.class.getSimpleName());
    }

    static Stream<Named<IndexSettingExtractor[]>> requireOnlyOneExtractorPerSetting() {
        IndexSetting setting = TestIndexSetting.INTEGER;

        IndexSettingExtractor duplicateReference = null;
        for (IndexSettingExtractor extractor : INDIVIDUAL_EXTRACTORS) {
            if (extractor.setting().equals(setting)) {
                duplicateReference = extractor;
                break;
            }
        }
        assertThat(duplicateReference).isNotNull();

        IndexSettingExtractor duplicateType = IntegerExtractor.of(setting);
        assertThat(duplicateType).isNotSameAs(duplicateReference).isEqualTo(duplicateReference);

        IndexSettingExtractor otherType = StringExtractor.of(setting);
        assertThat(otherType).isNotInstanceOf(duplicateReference.getClass());

        return Stream.of(
                Named.of("duplicate reference", concat(duplicateReference, INDIVIDUAL_EXTRACTORS)),
                Named.of("duplicate type and setting", concat(duplicateType, INDIVIDUAL_EXTRACTORS)),
                Named.of("duplicate setting", concat(otherType, INDIVIDUAL_EXTRACTORS)));
    }

    @Nested
    class ExtractionTest {
        private static final SettingsAccessor ACCESSOR = settings(
                entry(TestIndexSetting.INTEGER, 42),
                entry(TestIndexSetting.BOOLEAN, false),
                entry(TestIndexSetting.DOUBLE, Math.PI),
                entry(TestIndexSetting.STRING, "foo"));

        private KnownIndexSettingRecords records;

        @BeforeEach
        void setup() {
            records = new KnownIndexSettingRecords();
        }

        @Test
        void extractForValidation() {
            for (IndexSettingExtractor extractor : INDIVIDUAL_EXTRACTORS) {
                records.upsert(extractor.extractForValidation(ACCESSOR));
            }

            KnownIndexSettingRecords extractedRecords = EXTRACTORS.extractForValidation(ACCESSOR);
            assertThat(extractedRecords).containsExactlyInAnyOrderElementsOf(records);
        }

        @Test
        void extractForAuthoritativeRead() {
            for (IndexSettingExtractor extractor : INDIVIDUAL_EXTRACTORS) {
                records.upsert(extractor.extractForAuthoritativeRead(ACCESSOR));
            }

            KnownIndexSettingRecords extractedRecords = EXTRACTORS.extractForAuthoritativeRead(ACCESSOR);
            assertThat(extractedRecords).containsExactlyInAnyOrderElementsOf(records);
        }
    }
}
