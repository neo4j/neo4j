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

import static org.assertj.core.api.InstanceOfAssertFactories.iterable;
import static org.neo4j.internal.schema.IndexSettingTestUtils.FAKE_VALUE;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.Lookup;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.SingleIndexSettingLookup.NameToEnumLookup;
import org.neo4j.internal.schema.SingleIndexSettingProcessorTest.SingleProcessorTestBase;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SingleIndexSettingLookupTest {
    public abstract static class SingleLookupTestBase extends SingleProcessorTestBase {
        protected final SingleIndexSettingLookup<?> lookup;

        protected SingleLookupTestBase(SingleIndexSettingLookup<?> lookup) {
            super(lookup);
            this.lookup = lookup;
        }

        @Test
        void incorrectType() {
            RecordWithSetting record = new Pending(setting, FAKE_VALUE, Values.NO_VALUE);

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(lookup.validator.type);
        }
    }

    @Nested
    class NameToEnumLookupTest extends SingleLookupTestBase {
        protected NameToEnumLookupTest() {
            super(NameToEnumLookup.allOf(TestIndexSetting.STRING, Lookup.class));
        }

        @ParameterizedTest
        @ValueSource(strings = {"INCORRECT", "VALUE", "foo"})
        void invalidValues(String value) {
            RecordWithSetting record = new Pending(setting, value, Values.utf8Value(value));

            ObjectAssert<InvalidValue> invalidValueAssert =
                    processForVerificationAndAssertRecord(record, InvalidValue.class);
            invalidValueAssert.extracting(RecordWithValue::value).isEqualTo(value);

            Set<String> valid = new HashSet<>();
            for (Lookup entry : Lookup.values()) {
                valid.add(entry.name());
            }
            invalidValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(Supplier::get, iterable(String.class))
                    .containsExactlyInAnyOrderElementsOf(valid);
        }

        @ParameterizedTest
        @EnumSource
        void validPendingValues(Lookup value) {
            Value storable = Values.utf8Value(value.name());
            RecordWithSetting record = new Pending(setting, value.name(), storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);
        }

        @ParameterizedTest
        @EnumSource
        void validValidValues(Lookup value) {
            Value storable = Values.utf8Value(value.name());
            RecordWithSetting record = new Valid(setting, value.name(), storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);
        }
    }
}
