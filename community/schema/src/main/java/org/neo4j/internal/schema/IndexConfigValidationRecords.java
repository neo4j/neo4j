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

import java.util.Comparator;
import org.eclipse.collections.api.factory.SortedSets;
import org.eclipse.collections.api.multimap.sortedset.MutableSortedSetMultimap;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.eclipse.collections.impl.block.factory.Predicates;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

public class IndexConfigValidationRecords {
    private final MutableSortedSetMultimap<State, IndexConfigValidationRecord> records =
            Multimaps.mutable.sortedSet.with(IndexConfigValidationRecord.COMPARATOR);

    public IndexConfigValidationRecords with(IndexConfigValidationRecord record) {
        records.put(record.state(), record);
        return this;
    }

    public ImmutableSortedSet<IndexConfigValidationRecord> get(State state) {
        return records.get(state).toImmutableSortedSet();
    }

    public boolean invalid() {
        return records.keysView()
                .asLazy()
                .select(Predicates.notEqual(State.VALID))
                .notEmpty();
    }

    public boolean valid() {
        return !invalid();
    }

    public ImmutableSortedSet<Valid> validRecords() {
        return records.get(State.VALID).asLazy().collect(Valid.class::cast).toImmutableSortedSet();
    }

    public enum State {
        VALID,
        PENDING,
        UNRECOGNIZED_SETTING,
        MISSING_SETTING,
        INCORRECT_TYPE,
        INVALID_VALUE;

        public static final ImmutableSortedSet<State> INVALID_STATES =
                SortedSets.mutable.of(State.values()).without(VALID).toImmutableSortedSet();
    }

    public interface NamedSetting {
        String settingName();
    }

    public interface KnownSetting extends NamedSetting {
        IndexSetting setting();

        default String settingName() {
            return setting().getSettingName();
        }
    }

    public sealed interface IndexConfigValidationRecord extends NamedSetting, Comparable<IndexConfigValidationRecord>
            permits Valid, Invalid {
        Comparator<IndexConfigValidationRecord> COMPARATOR = Comparator.comparing(IndexConfigValidationRecord::state)
                .thenComparing(NamedSetting::settingName, CASE_INSENSITIVE_ORDER);

        State state();

        default int compareTo(IndexConfigValidationRecord other) {
            return COMPARATOR.compare(this, other);
        }
    }

    public record Valid(IndexSetting setting, Object value, Value stored)
            implements KnownSetting, IndexConfigValidationRecord {
        public Valid(Pending pending, Value stored) {
            this(pending.setting, pending.value, stored);
        }

        @Override
        public State state() {
            return State.VALID;
        }

        public <T> T get() {
            return (T) value;
        }
    }

    public sealed interface Invalid extends IndexConfigValidationRecord
            permits Pending, UnrecognizedSetting, MissingSetting, IncorrectType, InvalidValue {}

    public record Pending(IndexSetting setting, AnyValue rawValue, Object value) implements KnownSetting, Invalid {
        public Pending(IndexSetting setting, AnyValue rawValue) {
            this(setting, rawValue, null);
        }

        public Pending(Pending pending, Object value) {
            this(pending.setting, pending.rawValue, value);
        }

        @Override
        public State state() {
            return State.PENDING;
        }

        public <T> T get() {
            return (T) value;
        }
    }

    public record UnrecognizedSetting(String settingName) implements Invalid {
        @Override
        public State state() {
            return State.UNRECOGNIZED_SETTING;
        }
    }

    public record MissingSetting(IndexSetting setting) implements KnownSetting, Invalid {
        @Override
        public State state() {
            return State.MISSING_SETTING;
        }
    }

    public record IncorrectType(IndexSetting setting, AnyValue rawValue, Class<?> targetType)
            implements KnownSetting, Invalid {
        public IncorrectType(Pending pending, Class<?> targetType) {
            this(pending.setting, pending.rawValue, targetType);
        }

        @Override
        public State state() {
            return State.INCORRECT_TYPE;
        }

        public Class<?> providedType() {
            return rawValue.getClass();
        }
    }

    public record InvalidValue(IndexSetting setting, AnyValue rawValue, Object value, Object valid)
            implements KnownSetting, Invalid {
        public InvalidValue(Pending pending, Object value, Object valid) {
            this(pending.setting, pending.rawValue, value, valid);
        }

        public InvalidValue(Pending pending, Object valid) {
            this(pending, pending.value, valid);
        }

        @Override
        public State state() {
            return State.INVALID_VALUE;
        }
    }
}
