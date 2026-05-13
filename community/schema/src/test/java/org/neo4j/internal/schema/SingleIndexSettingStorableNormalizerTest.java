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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.iterable;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.neo4j.internal.schema.IndexSettingTestUtils.FAKE_VALUE;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.Lookup;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.SingleIndexSettingProcessorTest.SingleProcessorTestBase;
import org.neo4j.internal.schema.SingleIndexSettingStorableNormalizer.EnumToNameStorableNormalizer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SingleIndexSettingStorableNormalizerTest {
    public abstract static class SingleStorableNormalizerTestBase extends SingleProcessorTestBase {
        protected final SingleIndexSettingStorableNormalizer<?> normalizer;

        protected SingleStorableNormalizerTestBase(SingleIndexSettingStorableNormalizer<?> normalizer) {
            super(normalizer);
            this.normalizer = normalizer;
        }

        @Test
        void passthroughInvalid() {
            RecordWithSetting record = new Pending(setting, FAKE_VALUE, Values.NO_VALUE);
            assertThat(processor.processForVerification(record)).isSameAs(record);
        }

        @Test
        void passthroughValidForAuthoritativeRead() {
            RecordWithSetting record = new Valid(setting, FAKE_VALUE, Values.NO_VALUE);
            assertThat(processor.processForAuthoritativeRead(record)).isSameAs(record);
        }
    }

    @Nested
    class EnumToNameStorableNormalizerTest extends SingleStorableNormalizerTestBase {
        public static final Set<Lookup> LOOKUP;

        static {
            Set<Lookup> lookup = EnumSet.allOf(Lookup.class);
            lookup.remove(Lookup.BAR);
            LOOKUP = Collections.unmodifiableSet(lookup);
        }

        protected EnumToNameStorableNormalizerTest() {
            super(EnumToNameStorableNormalizer.of(TestIndexSetting.STRING, Lookup.class, LOOKUP));
        }

        @ParameterizedTest
        @NullSource
        @EnumSource(names = "BAR")
        void invalidValues(Lookup value) {
            RecordWithSetting record = new Valid(setting, value, Values.NO_VALUE);

            processForVerificationAndAssertRecord(record, InvalidValue.class)
                    .extracting(InvalidValue::requirement)
                    .extracting(Supplier::get, iterable(Lookup.class))
                    .containsExactlyInAnyOrderElementsOf(LOOKUP);
        }

        @Test
        void incorrectType() {
            RecordWithSetting record = new Valid(setting, FAKE_VALUE, Values.NO_VALUE);

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(normalizer.fromType);
        }

        @ParameterizedTest
        @EnumSource(mode = EXCLUDE, names = "BAR")
        void validValuesForVerification(Lookup value) {
            Value storable = Values.doubleValue(Math.PI); // doesn't matter
            Value processedStorable = Values.utf8Value(value.name());
            RecordWithSetting record = new Valid(setting, value, storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, processedStorable);
        }
    }
}
