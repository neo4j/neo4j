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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;

/// A [SingleIndexSettingProcessor] that has a lookup from one type to another
///
/// Naturally being a lookup, there will be a natural valid domain and codomain
/// thus should hold the [ValidatingIndexSettingsProcessor] contract.
public abstract class SingleIndexSettingLookup<FROM> extends SingleIndexSettingProcessor
        implements ValidatingIndexSettingsProcessor {
    protected final SingleIndexSettingValidator<FROM> validator;
    protected final SingleIndexSettingConverter<FROM> converter;

    protected SingleIndexSettingLookup(
            IndexSetting setting, Class<FROM> fromType, IndexSettingsRequirement<?> requirement) {
        super(setting);
        this.validator = new Validator(setting, fromType, requirement);
        this.converter = new Converter(setting, fromType);
    }

    @Override
    public RecordWithSetting processForVerification(RecordWithSetting record) {
        return converter.processForVerification(validator.processForVerification(record));
    }

    @Override
    public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
        return converter.processForAuthoritativeRead(record);
    }

    protected boolean contains(FROM value) {
        return lookup(value) != null;
    }

    protected abstract Object lookup(FROM value);

    private class Validator extends SingleIndexSettingValidator<FROM> {
        private Validator(IndexSetting setting, Class<FROM> type, IndexSettingsRequirement<?> requirement) {
            super(setting, type, requirement);
        }

        @Override
        protected boolean isValid(FROM value) {
            return contains(value);
        }
    }

    private class Converter extends SingleIndexSettingConverter<FROM> {
        private Converter(IndexSetting setting, Class<FROM> fromType) {
            super(setting, fromType);
        }

        @Override
        protected Object convert(FROM value) {
            return lookup(value);
        }
    }

    // =================
    //  Implementations
    // =================

    public static class SingleIndexSettingMapLookup<FROM> extends SingleIndexSettingLookup<FROM> {
        protected final Map<FROM, ?> lookup;

        public static <FROM> SingleIndexSettingMapLookup<FROM> of(
                IndexSetting setting, Class<FROM> fromType, Map<FROM, ?> lookup) {
            return new SingleIndexSettingMapLookup<>(setting, fromType, lookup);
        }

        protected SingleIndexSettingMapLookup(IndexSetting setting, Class<FROM> fromType, Map<FROM, ?> lookup) {
            super(setting, fromType, new IterableRequirement(lookup.keySet()));
            this.lookup = Collections.unmodifiableMap(lookup);
        }

        @Override
        protected boolean contains(FROM value) {
            return lookup.containsKey(value);
        }

        @Override
        protected Object lookup(FROM value) {
            return lookup.get(value);
        }
    }

    public static final class NameToEnumLookup extends SingleIndexSettingMapLookup<String> {
        public static <TYPE extends Enum<TYPE>> NameToEnumLookup allOf(IndexSetting setting, Class<TYPE> type) {
            return of(setting, EnumSet.allOf(type));
        }

        @SafeVarargs
        public static <TYPE extends Enum<TYPE>> NameToEnumLookup of(IndexSetting setting, TYPE first, TYPE... rest) {
            return of(setting, EnumSet.of(first, rest));
        }

        public static <TYPE extends Enum<TYPE>> NameToEnumLookup of(IndexSetting setting, Set<TYPE> values) {
            Map<String, TYPE> lookup = new TreeMap<>();
            for (TYPE value : values) {
                lookup.put(value.name(), value);
            }
            return new NameToEnumLookup(setting, lookup);
        }

        private NameToEnumLookup(IndexSetting setting, Map<String, ?> lookup) {
            super(setting, String.class, lookup);
        }
    }
}
