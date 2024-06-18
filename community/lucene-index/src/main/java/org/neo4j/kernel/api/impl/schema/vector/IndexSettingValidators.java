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

import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.missing;

import java.util.Locale;
import org.eclipse.collections.api.map.MapIterable;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.IncorrectType;
import org.neo4j.internal.schema.IndexConfigValidationRecords.IndexConfigValidationRecord;
import org.neo4j.internal.schema.IndexConfigValidationRecords.InvalidValue;
import org.neo4j.internal.schema.IndexConfigValidationRecords.MissingSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.Pending;
import org.neo4j.internal.schema.IndexConfigValidationRecords.UnrecognizedSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.Valid;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.Range;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class IndexSettingValidators {
    abstract static class IndexSettingValidator<VALUE extends Value, TYPE> {
        protected final IndexSetting setting;
        protected final TYPE createDefault;
        protected final TYPE readDefault;

        protected IndexSettingValidator(IndexSetting setting) {
            this(setting, null);
        }

        protected IndexSettingValidator(IndexSetting setting, TYPE defaultValue) {
            this(setting, defaultValue, defaultValue);
        }

        protected IndexSettingValidator(IndexSetting setting, TYPE readDefault, TYPE createDefault) {
            this.setting = setting;
            this.readDefault = readDefault;
            this.createDefault = createDefault;
        }

        abstract TYPE map(VALUE value);

        abstract Value map(TYPE value);

        abstract IndexConfigValidationRecord validate(SettingsAccessor accessor);

        protected IndexConfigValidationRecord extractOrDefault(SettingsAccessor accessor) {
            final var rawValue = accessor.get(setting);
            if (!missing(rawValue)) {
                return new Pending(setting, rawValue);
            }
            if (createDefault == null) {
                return new MissingSetting(setting);
            }
            return new Valid(setting, createDefault, map(createDefault));
        }

        protected Valid trustIsValid(SettingsAccessor accessor) {
            final VALUE rawValue = accessor.get(setting);
            final var value = missing(rawValue) ? readDefault : map(rawValue);
            return new Valid(setting, value, rawValue);
        }

        protected IndexSetting setting() {
            return setting;
        }
    }

    static class ReadDefaultOnly<TYPE> extends IndexSettingValidator<NoValue, TYPE> {
        protected ReadDefaultOnly(IndexSetting setting, TYPE readDefault) {
            super(setting, readDefault, null);
        }

        @Override
        TYPE map(NoValue value) {
            return readDefault;
        }

        @Override
        NoValue map(TYPE value) {
            return NoValue.NO_VALUE;
        }

        @Override
        IndexConfigValidationRecord validate(SettingsAccessor accessor) {
            final var rawValue = accessor.get(setting);
            if (!missing(rawValue)) {
                return new UnrecognizedSetting(setting.getSettingName());
            }
            return new Valid(setting, readDefault, Values.NO_VALUE);
        }
    }

    static final class DimensionsValidator extends IndexSettingValidator<IntegralValue, Integer> {
        private final Range<Integer> supportedRange;

        DimensionsValidator(Range<Integer> supportedRange) {
            super(IndexSetting.vector_Dimensions());
            this.supportedRange = supportedRange;
        }

        @Override
        Integer map(IntegralValue dimensions) {
            return (int) dimensions.longValue();
        }

        @Override
        Value map(Integer dimensions) {
            return Values.intValue(dimensions);
        }

        @Override
        IndexConfigValidationRecord validate(SettingsAccessor accessor) {
            final var record = extractOrDefault(accessor);
            if (!(record instanceof final Pending pending)) {
                return record;
            }

            if (!(pending.rawValue() instanceof final IntegralValue integralValue)) {
                return new IncorrectType(pending, IntegralValue.class);
            }

            final var dimensions = map(integralValue);
            return supportedRange.contains(dimensions)
                    ? new Valid(setting, dimensions, map(dimensions))
                    : new InvalidValue(pending, dimensions, supportedRange);
        }
    }

    static final class SimilarityFunctionValidator extends IndexSettingValidator<TextValue, VectorSimilarityFunction> {
        private final MapIterable<String, VectorSimilarityFunction> similarityFunctions;

        SimilarityFunctionValidator(MapIterable<String, VectorSimilarityFunction> supportedSimilarityFunctions) {
            super(IndexSetting.vector_Similarity_Function());
            this.similarityFunctions = supportedSimilarityFunctions;
        }

        @Override
        VectorSimilarityFunction map(TextValue textValue) {
            return similarityFunctions.get(textValue.stringValue().toUpperCase(Locale.ROOT));
        }

        @Override
        Value map(VectorSimilarityFunction similarityFunction) {
            return Values.stringValue(similarityFunction.name());
        }

        @Override
        IndexConfigValidationRecord validate(SettingsAccessor accessor) {
            final var record = extractOrDefault(accessor);
            if (!(record instanceof final Pending pending)) {
                return record;
            }

            if (!(pending.rawValue() instanceof final TextValue textValue)) {
                return new IncorrectType(pending, TextValue.class);
            }

            final var similarityFunction = similarityFunctions.get(textValue.stringValue());
            return similarityFunction == null
                    ? new InvalidValue(pending, similarityFunctions.keysView())
                    : new Valid(setting, similarityFunction, map(similarityFunction));
        }
    }
}
