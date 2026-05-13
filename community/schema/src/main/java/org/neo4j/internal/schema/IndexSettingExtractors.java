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

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;
import static org.neo4j.internal.schema.IndexConfigUtils.duplicateSettings;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.InclusiveRange;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Unprocessed;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsRequirements.DefaultRequirement;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

/// A collection of [IndexSettingExtractor]s
public class IndexSettingExtractors {
    private final Map<IndexSetting, IndexSettingExtractor> extractors;
    private final Set<String> settingNames;

    public IndexSettingExtractors(IndexSettingExtractor... extractors) {
        SortedMap<IndexSetting, IndexSettingExtractor> sortedExtractors = new TreeMap<>(INDEX_SETTING_COMPARATOR);
        SortedSet<String> settingNames = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        SortedSet<String> duplicateSettings = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        for (IndexSettingExtractor extractor : extractors) {
            IndexSetting setting = extractor.setting();
            String settingName = setting.getSettingName();
            if (!settingNames.add(settingName)) {
                duplicateSettings.add(settingName);
            }
            sortedExtractors.put(setting, extractor);
        }
        if (!duplicateSettings.isEmpty()) {
            throw duplicateSettings(
                    IndexSettingExtractor.class.getSimpleName(), IndexSetting.class.getSimpleName(), duplicateSettings);
        }

        this.extractors = Collections.unmodifiableSortedMap(sortedExtractors);
        this.settingNames = Collections.unmodifiableSortedSet(settingNames);
    }

    /// Extracts the values from the [SettingsAccessor] into a mutable collection
    /// @see IndexSettingExtractor#extractForValidation(SettingsAccessor)
    public KnownIndexSettingRecords extractForValidation(SettingsAccessor accessor) {
        KnownIndexSettingRecords records = new KnownIndexSettingRecords();
        for (IndexSettingExtractor extractor : extractors.values()) {
            records.upsert(extractor.extractForValidation(accessor));
        }
        return records;
    }

    /// Extracts the values from the [SettingsAccessor] into a mutable collection
    /// @see IndexSettingExtractor#extractForAuthoritativeRead(SettingsAccessor)
    public KnownIndexSettingRecords extractForAuthoritativeRead(SettingsAccessor accessor) {
        KnownIndexSettingRecords records = new KnownIndexSettingRecords();
        for (IndexSettingExtractor extractor : extractors.values()) {
            records.upsert(extractor.extractForAuthoritativeRead(accessor));
        }
        return records;
    }

    Set<IndexSetting> settings() {
        return extractors.keySet();
    }

    Set<String> settingNames() {
        return settingNames;
    }

    @Override
    public String toString() {
        return Iterables.toString(extractors.values(), ", ", getClass().getSimpleName() + "[", "]");
    }

    // =======================================
    // IndexSettingExtractor implementations:
    // =======================================

    private abstract static class RawIndexSettingExtractor implements IndexSettingExtractor {
        private final IndexSetting setting;

        protected RawIndexSettingExtractor(IndexSetting setting) {
            this.setting = setting;
        }

        @Override
        public IndexSetting setting() {
            return setting;
        }

        protected RecordWithSetting extractRawValue(SettingsAccessor accessor) {
            if (!accessor.containsSetting(setting)) {
                return new MissingSetting(setting);
            }

            AnyValue value = accessor.get(setting);
            if (value == null || value == Values.NO_VALUE) {
                return new MissingSetting(setting);
            }

            return new Unprocessed(setting, value);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + settingName() + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(setting);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            RawIndexSettingExtractor that = (RawIndexSettingExtractor) obj;
            return Objects.equals(this.setting, that.setting);
        }
    }

    public static final class BooleanExtractor extends RawIndexSettingExtractor {
        public static BooleanExtractor of(IndexSetting setting) {
            return new BooleanExtractor(setting);
        }

        private BooleanExtractor(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting extractForValidation(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }
            if (!(unprocessed.rawValue() instanceof BooleanValue booleanValue)) {
                return new IncorrectType(unprocessed, BooleanValue.class);
            }

            return new Pending(unprocessed, booleanValue.booleanValue(), booleanValue);
        }

        @Override
        public RecordWithSetting extractForAuthoritativeRead(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }

            BooleanValue booleanValue = (BooleanValue) unprocessed.rawValue();
            return new Valid(unprocessed, booleanValue.booleanValue(), booleanValue);
        }
    }

    public static final class IntegerExtractor extends RawIndexSettingExtractor {
        private static final InclusiveRange<Long> INTEGER_RANGE =
                new InclusiveRange<>((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE);

        public static IntegerExtractor of(IndexSetting setting) {
            return new IntegerExtractor(setting);
        }

        private IntegerExtractor(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting extractForValidation(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }
            if (!(unprocessed.rawValue() instanceof IntegralValue integralValue)) {
                return new IncorrectType(unprocessed, IntegralValue.class);
            }

            if (integralValue instanceof IntValue intValue) {
                return new Pending(unprocessed, intValue.intValue(), intValue);
            }

            int value;
            if (integralValue instanceof LongValue longValue) {
                // LongValue::intValue will always throw, go via long
                long primitiveLong = longValue.longValue();
                if (!INTEGER_RANGE.contains(primitiveLong)) {
                    return new InvalidValue(unprocessed, primitiveLong, new DefaultRequirement<>(INTEGER_RANGE));
                }
                value = (int) primitiveLong;
            } else {
                value = integralValue.intValue();
            }

            return new Pending(unprocessed, value, Values.intValue(value));
        }

        @Override
        public RecordWithSetting extractForAuthoritativeRead(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }

            IntegralValue integralValue = (IntegralValue) unprocessed.rawValue();
            return new Valid(unprocessed, (int) integralValue.longValue(), integralValue);
        }
    }

    public static final class DoubleExtractor extends RawIndexSettingExtractor {
        public static DoubleExtractor of(IndexSetting setting) {
            return new DoubleExtractor(setting);
        }

        private DoubleExtractor(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting extractForValidation(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }
            if (!(unprocessed.rawValue() instanceof NumberValue numberValue)) {
                return new IncorrectType(unprocessed, NumberValue.class);
            }

            if (numberValue instanceof DoubleValue doubleValue) {
                return new Pending(unprocessed, doubleValue.doubleValue(), doubleValue);
            }

            double value = numberValue.doubleValue();
            return new Pending(unprocessed, value, Values.doubleValue(value));
        }

        @Override
        public RecordWithSetting extractForAuthoritativeRead(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }

            NumberValue numberValue = (NumberValue) unprocessed.rawValue();
            return new Valid(unprocessed, numberValue.doubleValue(), numberValue);
        }
    }

    public static final class StringExtractor extends RawIndexSettingExtractor {
        public static StringExtractor of(IndexSetting setting) {
            return new StringExtractor(setting);
        }

        private StringExtractor(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting extractForValidation(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }
            if (!(unprocessed.rawValue() instanceof TextValue textValue)) {
                return new IncorrectType(unprocessed, TextValue.class);
            }

            return new Pending(unprocessed, textValue.stringValue(), textValue);
        }

        @Override
        public RecordWithSetting extractForAuthoritativeRead(SettingsAccessor accessor) {
            RecordWithSetting record = extractRawValue(accessor);
            if (!(record instanceof Unprocessed unprocessed)) {
                return record;
            }

            TextValue textValue = (TextValue) unprocessed.rawValue();
            return new Valid(unprocessed, textValue.stringValue(), textValue);
        }
    }
}
