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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.CLASS;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.util.StringJoiner;
import java.util.function.Function;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigUtils.HasSetting;
import org.neo4j.internal.schema.IndexConfigUtils.NamedSetting;
import org.neo4j.internal.schema.IndexSettingRecord;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecord.UnrecognizedSetting;
import org.neo4j.internal.schema.IndexSettingRecordsByState;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.TypedIndexSettingsValidator;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.Value;

class VectorIndexConfigValidationTestUtils {

    static AbstractThrowableAssert<?, InvalidArgumentException> assertInvalidArgumentExceptionThrownBy(
            ThrowingCallable callable) {
        return assertThatThrownBy(callable)
                .asInstanceOf(InstanceOfAssertFactories.throwable(InvalidArgumentException.class));
    }

    static <T> T assertAndReturnFunctionDoesNotThrow(ThrowingSupplier<T> supplier) {
        Mutable<T> ref = new MutableObject<>();
        assertThatCode(() -> ref.setValue(supplier.get())).doesNotThrowAnyException();
        return ref.get();
    }

    static IndexSettingRecordsByState validateAsValid(
            TypedIndexSettingsValidator<VectorIndexConfig> validator, SettingsAccessor accessor) {
        IndexSettingRecordsByState records = validator.validate(accessor);
        assertThat(records.valid()).isTrue();
        return records;
    }

    static ObjectAssert<VectorIndexConfig> assertVectorIndexConfigSetting(
            VectorIndexConfig vectorIndexConfig,
            IndexSetting setting,
            Function<VectorIndexConfig, Object> accessor,
            Object value,
            Value storable) {
        ObjectAssert<VectorIndexConfig> vectorIndexConfigAssert = assertThat(vectorIndexConfig);
        vectorIndexConfigAssert
                .extracting(accessor, config -> config.get(setting), config -> config.getValue(setting))
                .containsExactly(value, value, storable);
        return vectorIndexConfigAssert;
    }

    static IndexSettingRecordsByState validateAsInvalid(
            TypedIndexSettingsValidator<VectorIndexConfig> validator, SettingsAccessor accessor) {
        IndexSettingRecordsByState records = validator.validate(accessor);
        assertThat(records.invalid()).isTrue();
        return records;
    }

    static ObjectAssert<UnrecognizedSetting> assertUnrecognizedSetting(
            IndexSettingRecordsByState records, String settingName) {
        ObjectAssert<UnrecognizedSetting> unrecognizedSettingAssert =
                assertSingleRecordOfType(records, State.UNRECOGNIZED_SETTING, UnrecognizedSetting.class);
        unrecognizedSettingAssert.extracting(NamedSetting::settingName).isEqualTo(settingName);
        return unrecognizedSettingAssert;
    }

    static ObjectAssert<MissingSetting> assertMissingSetting(IndexSettingRecordsByState records, IndexSetting setting) {
        ObjectAssert<MissingSetting> missingSettingAssert =
                assertSingleRecordOfType(records, State.MISSING_SETTING, MissingSetting.class);
        missingSettingAssert.extracting(HasSetting::setting).isEqualTo(setting);
        return missingSettingAssert;
    }

    static ObjectAssert<IncorrectType> assertIncorrectType(
            IndexSettingRecordsByState records,
            IndexSetting setting,
            Object invalidValue,
            Class<?> providedType,
            Class<?> targetType) {
        ObjectAssert<IncorrectType> incorrectTypeAssert =
                assertSingleRecordOfType(records, State.INCORRECT_TYPE, IncorrectType.class);
        incorrectTypeAssert
                .extracting(HasSetting::setting, RecordWithValue::value)
                .containsExactly(setting, invalidValue);
        incorrectTypeAssert.extracting(IncorrectType::providedType, CLASS).isAssignableTo(providedType);
        incorrectTypeAssert.extracting(IncorrectType::targetType, CLASS).isEqualTo(targetType);
        return incorrectTypeAssert;
    }

    static ObjectAssert<InvalidValue> assertInvalidValue(
            IndexSettingRecordsByState records, IndexSetting setting, Object invalidValue) {
        ObjectAssert<InvalidValue> invalidValueAssert =
                assertSingleRecordOfType(records, State.INVALID_VALUE, InvalidValue.class);
        invalidValueAssert
                .extracting(HasSetting::setting, RecordWithValue::value)
                .containsExactly(setting, invalidValue);
        return invalidValueAssert;
    }

    private static <T extends IndexSettingRecord> ObjectAssert<T> assertSingleRecordOfType(
            IndexSettingRecordsByState records, State state, Class<T> type) {
        return assertThat(records.get(state)).singleElement(type(type));
    }

    static String similarityFunctionsToString(Iterable<? extends VectorSimilarityFunction> similarityFunctions) {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (VectorSimilarityFunction similarityFunction : similarityFunctions) {
            joiner.add(similarityFunction.functionName());
        }
        return joiner.toString();
    }
}
