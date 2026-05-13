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

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/// A [SingleIndexSettingProcessor] which normalizes the storable field of a [Valid] record
public abstract class SingleIndexSettingStorableNormalizer<FROM> extends SingleIndexSettingProcessor {
    protected final Class<FROM> fromType;
    protected final IndexSettingsRequirement<?> requirement;

    protected SingleIndexSettingStorableNormalizer(
            IndexSetting setting, Class<FROM> fromType, IndexSettingsRequirement<?> requirement) {
        super(setting);
        this.fromType = fromType;
        this.requirement = requirement;
    }

    protected boolean normalizable(FROM value) {
        return normalize(value) != null;
    }

    protected abstract Value normalize(FROM value);

    @Override
    public RecordWithSetting processForVerification(RecordWithSetting record) {
        if (!(record instanceof Valid valid)) {
            return record;
        }

        Object value = valid.value();
        if (value == null) {
            return new InvalidValue(valid, requirement);
        }
        if (!fromType.isInstance(value)) {
            return new IncorrectType(valid, fromType);
        }
        FROM typedValue = valid.valueAs(fromType);
        if (!normalizable(typedValue)) {
            return new InvalidValue(valid, requirement);
        }

        return new Valid(valid, typedValue, normalize(typedValue));
    }

    @Override
    public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
        return record;
    }

    /// A [SingleIndexSettingStorableNormalizer] that allows for `null` values within records
    public abstract static class SingleIndexSettingNullableStorableNormalizer<FROM>
            extends SingleIndexSettingStorableNormalizer<FROM> {
        protected SingleIndexSettingNullableStorableNormalizer(
                IndexSetting setting, Class<FROM> fromType, IndexSettingsRequirement<?> requirement) {
            super(setting, fromType, requirement);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof Valid valid)) {
                return record;
            }

            Object value = valid.value();
            if (value != null && !fromType.isInstance(value)) {
                return new IncorrectType(valid, fromType);
            }
            FROM typedValue = valid.valueAs(fromType);
            if (!normalizable(typedValue)) {
                return new InvalidValue(valid, requirement);
            }

            return new Valid(valid, typedValue, normalize(typedValue));
        }
    }

    // =================
    //  Implementations
    // =================

    public static class SingleIndexSettingMapStorableNormalizer<FROM>
            extends SingleIndexSettingStorableNormalizer<FROM> {
        protected final Map<FROM, Value> lookup;

        public static <FROM> SingleIndexSettingMapStorableNormalizer<FROM> of(
                IndexSetting setting, Class<FROM> fromType, Map<FROM, ? extends Value> lookup) {
            return new SingleIndexSettingMapStorableNormalizer<>(setting, fromType, lookup);
        }

        protected SingleIndexSettingMapStorableNormalizer(
                IndexSetting setting, Class<FROM> fromType, Map<FROM, ? extends Value> lookup) {
            super(setting, fromType, new IterableRequirement(lookup.keySet()));
            this.lookup = Collections.unmodifiableMap(lookup);
        }

        @Override
        protected boolean normalizable(FROM value) {
            return lookup.containsKey(value);
        }

        @Override
        protected Value normalize(FROM value) {
            return lookup.get(value);
        }
    }

    public static final class EnumToNameStorableNormalizer<FROM extends Enum<FROM>>
            extends SingleIndexSettingMapStorableNormalizer<FROM> {
        public static <FROM extends Enum<FROM>> EnumToNameStorableNormalizer<FROM> allOf(
                IndexSetting setting, Class<FROM> fromType) {
            return of(setting, fromType, EnumSet.allOf(fromType));
        }

        @SafeVarargs
        public static <FROM extends Enum<FROM>> EnumToNameStorableNormalizer<FROM> of(
                IndexSetting setting, Class<FROM> fromType, FROM first, FROM... rest) {
            return of(setting, fromType, EnumSet.of(first, rest));
        }

        public static <FROM extends Enum<FROM>> EnumToNameStorableNormalizer<FROM> of(
                IndexSetting setting, Class<FROM> fromType, Set<FROM> values) {
            Map<FROM, TextValue> lookup = new EnumMap<>(fromType);
            for (FROM value : values) {
                lookup.put(value, Values.utf8Value(value.name()));
            }
            return new EnumToNameStorableNormalizer<>(setting, fromType, lookup);
        }

        private EnumToNameStorableNormalizer(
                IndexSetting setting, Class<FROM> fromType, Map<FROM, ? extends Value> lookup) {
            super(setting, fromType, lookup);
        }
    }
}
