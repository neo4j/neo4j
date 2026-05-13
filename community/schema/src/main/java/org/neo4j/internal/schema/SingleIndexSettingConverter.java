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

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsRequirements.ClassRequirement;

/// A [SingleIndexSettingProcessor] which converts/transforms a value to another with the same [IndexSetting]
public abstract class SingleIndexSettingConverter<FROM> extends SingleIndexSettingProcessor {
    protected final Class<FROM> fromType;

    protected SingleIndexSettingConverter(IndexSetting setting, Class<FROM> fromType) {
        super(setting);
        this.fromType = fromType;
    }

    protected abstract Object convert(FROM value);

    /// Converts values from [RecordWithStorable] using [#convert(FROM)]
    @Override
    public RecordWithSetting processForVerification(RecordWithSetting record) {
        if (!(record instanceof RecordWithStorable hasStorable)) {
            return record;
        }

        Object value = hasStorable.value();
        if (value == null) {
            return new InvalidValue(hasStorable, new ClassRequirement(fromType));
        }
        if (!fromType.isInstance(value)) {
            return new IncorrectType(hasStorable, fromType);
        }

        Object convertedValue = convert(hasStorable.valueAs(fromType));
        return switch (hasStorable) {
            case Pending pending -> new Pending(pending, convertedValue);
            case Valid valid -> new Valid(valid, convertedValue);
        };
    }

    @Override
    public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
        if (!(record instanceof Valid valid)) {
            return record;
        }

        Object convertedValue = convert(valid.valueAs(fromType));
        return new Valid(valid, convertedValue);
    }

    /// A [SingleIndexSettingConverter] that allows for `null` values within records
    public abstract static class SingleIndexSettingNullableConverter<FROM> extends SingleIndexSettingConverter<FROM> {
        protected SingleIndexSettingNullableConverter(IndexSetting setting, Class<FROM> fromType) {
            super(setting, fromType);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof RecordWithStorable hasStorable)) {
                return record;
            }

            Object value = hasStorable.value();
            if (value != null && !fromType.isInstance(value)) {
                return new IncorrectType(hasStorable, fromType);
            }

            Object convertedValue = convert(hasStorable.valueAs(fromType));
            return switch (hasStorable) {
                case Pending pending -> new Pending(pending, convertedValue);
                case Valid valid -> new Valid(valid, convertedValue);
            };
        }
    }

    // =================
    //  Implementations
    // =================

    public static final class IntegerToOptionalIntConverter extends SingleIndexSettingNullableConverter<Integer> {
        public static IntegerToOptionalIntConverter of(IndexSetting setting) {
            return new IntegerToOptionalIntConverter(setting);
        }

        private IntegerToOptionalIntConverter(IndexSetting setting) {
            super(setting, Integer.class);
        }

        @Override
        protected OptionalInt convert(Integer value) {
            return value != null ? OptionalInt.of(value) : OptionalInt.empty();
        }
    }

    public static final class TypeToOptionalConverter<FROM> extends SingleIndexSettingNullableConverter<FROM> {
        public static <FROM> TypeToOptionalConverter<FROM> of(IndexSetting setting, Class<FROM> fromType) {
            return new TypeToOptionalConverter<>(setting, fromType);
        }

        private TypeToOptionalConverter(IndexSetting setting, Class<FROM> fromType) {
            super(setting, fromType);
        }

        @Override
        protected Optional<FROM> convert(FROM value) {
            return Optional.ofNullable(value);
        }
    }

    public static final class StringToUpperCaseConverter extends SingleIndexSettingConverter<String> {
        private final Locale locale;

        public static StringToUpperCaseConverter of(IndexSetting setting) {
            return of(setting, Locale.ROOT);
        }

        public static StringToUpperCaseConverter of(IndexSetting setting, Locale locale) {
            return new StringToUpperCaseConverter(setting, locale);
        }

        private StringToUpperCaseConverter(IndexSetting setting, Locale locale) {
            super(setting, String.class);
            this.locale = locale;
        }

        @Override
        protected Object convert(String value) {
            return value.toUpperCase(locale);
        }
    }
}
