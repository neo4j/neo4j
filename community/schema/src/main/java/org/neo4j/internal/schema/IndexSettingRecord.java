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

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigUtils.HasSetting;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexConfigUtils.NamedSetting;
import org.neo4j.util.MarkerInterface;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

public sealed interface IndexSettingRecord extends NamedSetting, Comparable<IndexSettingRecord> {
    Comparator<IndexSettingRecord> COMPARATOR = Comparator.comparing(IndexSettingRecord::state)
            .thenComparing(NamedSetting::settingName, CASE_INSENSITIVE_ORDER);

    State state();

    @Override
    default int compareTo(IndexSettingRecord other) {
        return COMPARATOR.compare(this, other);
    }

    enum State {
        VALID,
        UNPROCESSED,
        PENDING,
        UNRECOGNIZED_SETTING,
        MISSING_SETTING,
        INCORRECT_TYPE,
        INVALID_VALUE;

        public static final SortedSet<State> INVALID_STATES;

        static {
            SortedSet<State> invalidStates = new TreeSet<>();
            for (State state : values()) {
                if (state != VALID) {
                    invalidStates.add(state);
                }
            }
            INVALID_STATES = Collections.unmodifiableSortedSet(invalidStates);
        }
    }

    sealed interface RecordWithSetting extends IndexSettingRecord, HasSetting {}

    sealed interface RecordWithValue extends RecordWithSetting {
        Object value();

        @SuppressWarnings("unchecked")
        default <T> T valueAs(Class<T> type) {
            return (T) value();
        }

        @SuppressWarnings("unchecked")
        default <T> T get() {
            return (T) value();
        }
    }

    sealed interface RecordWithStorable extends RecordWithValue {
        Value storable();
    }

    record Valid(IndexSetting setting, Object value, Value storable) implements RecordWithStorable {
        public Valid(RecordWithStorable hasStorable) {
            this(hasStorable.setting(), hasStorable.value(), hasStorable.storable());
        }

        public Valid(RecordWithStorable hasStorable, Object value) {
            this(hasStorable.setting(), value, hasStorable.storable());
        }

        public Valid(RecordWithValue hasValue, Value storable) {
            this(hasValue.setting(), hasValue.value(), storable);
        }

        public Valid(IndexSettingEntry entry) {
            this(entry, entry.value(), entry.storable());
        }

        public Valid(HasSetting hasSetting, Object value, Value storable) {
            this(hasSetting.setting(), value, storable);
        }

        @Override
        public State state() {
            return State.VALID;
        }
    }

    @MarkerInterface
    sealed interface Invalid extends IndexSettingRecord {}

    record Unprocessed(IndexSetting setting, AnyValue rawValue) implements RecordWithSetting, Invalid {
        @Override
        public State state() {
            return State.UNPROCESSED;
        }
    }

    record Pending(IndexSetting setting, Object value, Value storable) implements RecordWithStorable, Invalid {
        public Pending(RecordWithStorable hasStorable) {
            this(hasStorable.setting(), hasStorable.value(), hasStorable.storable());
        }

        public Pending(RecordWithStorable hasStorable, Object value) {
            this(hasStorable.setting(), value, hasStorable.storable());
        }

        public Pending(RecordWithValue hasValue, Value storable) {
            this(hasValue.setting(), hasValue.value(), storable);
        }

        public Pending(HasSetting hasSetting, Object value, Value storable) {
            this(hasSetting.setting(), value, storable);
        }

        @Override
        public State state() {
            return State.PENDING;
        }
    }

    record UnrecognizedSetting(String settingName) implements Invalid {
        public UnrecognizedSetting(IndexSetting setting) {
            this(setting.getSettingName());
        }

        public UnrecognizedSetting(NamedSetting namedSetting) {
            this(namedSetting.settingName());
        }

        @Override
        public State state() {
            return State.UNRECOGNIZED_SETTING;
        }
    }

    record MissingSetting(IndexSetting setting) implements RecordWithSetting, Invalid {
        public MissingSetting(HasSetting hasSetting) {
            this(hasSetting.setting());
        }

        @Override
        public State state() {
            return State.MISSING_SETTING;
        }
    }

    record IncorrectType(IndexSetting setting, Object value, Class<?> targetType) implements RecordWithValue, Invalid {
        public IncorrectType(HasSetting hasSetting, Object value, Class<?> targetType) {
            this(hasSetting.setting(), value, targetType);
        }

        public IncorrectType(RecordWithValue hasValue, Class<?> targetType) {
            this(hasValue.setting(), hasValue.value(), targetType);
        }

        public IncorrectType(Unprocessed unprocessed, Class<?> targetType) {
            this(unprocessed.setting, unprocessed.rawValue, targetType);
        }

        @Override
        public State state() {
            return State.INCORRECT_TYPE;
        }

        public Class<?> providedType() {
            return value.getClass();
        }
    }

    record InvalidValue(IndexSetting setting, Object value, IndexSettingsRequirement<?> requirement)
            implements RecordWithValue, Invalid {
        public InvalidValue(RecordWithValue hasValue, IndexSettingsRequirement<?> requirement) {
            this(hasValue.setting(), hasValue.value(), requirement);
        }

        public InvalidValue(HasSetting hasSetting, Object value, IndexSettingsRequirement<?> requirement) {
            this(hasSetting.setting(), value, requirement);
        }

        @Override
        public State state() {
            return State.INVALID_VALUE;
        }
    }
}
