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

import java.util.OptionalInt;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.InclusiveRange;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsRequirements.DefaultRequirement;

/// A [SingleIndexSettingProcessor] which validates the value and produces a [Valid] record.
public abstract class SingleIndexSettingValidator<TYPE> extends SingleIndexSettingProcessor
        implements ValidatingIndexSettingsProcessor {
    protected final Class<TYPE> type;
    protected final IndexSettingsRequirement<?> requirement;

    protected SingleIndexSettingValidator(
            IndexSetting setting, Class<TYPE> type, IndexSettingsRequirement<?> requirement) {
        super(setting);
        this.type = type;
        this.requirement = requirement;
    }

    protected abstract boolean isValid(TYPE value);

    /// Validates values from [Pending] records using [#isValid(TYPE)]
    @Override
    public RecordWithSetting processForVerification(RecordWithSetting record) {
        if (!(record instanceof Pending pending)) {
            return record;
        }

        Object value = pending.value();
        if (value == null) {
            return new InvalidValue(pending, requirement);
        }
        if (!type.isInstance(value)) {
            return new IncorrectType(pending, type);
        }
        if (!isValid(pending.valueAs(type))) {
            return new InvalidValue(pending, requirement);
        }

        return new Valid(pending);
    }

    @Override
    public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
        return record;
    }

    /// A [SingleIndexSettingValidator] that allows for `null` values within records
    public abstract static class SingleIndexSettingNullableValidator<TYPE> extends SingleIndexSettingValidator<TYPE> {
        protected SingleIndexSettingNullableValidator(
                IndexSetting setting, Class<TYPE> type, IndexSettingsRequirement<?> requirement) {
            super(setting, type, requirement);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof Pending pending)) {
                return record;
            }

            Object value = pending.value();
            if (value != null && !type.isInstance(value)) {
                return new IncorrectType(pending, type);
            }
            if (!isValid(pending.valueAs(type))) {
                return new InvalidValue(pending, requirement);
            }

            return new Valid(pending);
        }
    }

    // =================
    //  Implementations
    // =================

    /// A [SingleIndexSettingValidator] for [Integer]
    ///
    /// Valid: non-null and inclusively within the provide range
    public static final class IntegerRangeValidator extends SingleIndexSettingValidator<Integer> {
        private final InclusiveRange<Integer> supportedRange;

        public static IntegerRangeValidator of(IndexSetting setting, int min, int max) {
            return new IntegerRangeValidator(setting, new InclusiveRange<>(min, max));
        }

        private IntegerRangeValidator(IndexSetting setting, InclusiveRange<Integer> supportedRange) {
            super(setting, Integer.class, new DefaultRequirement<>(supportedRange));
            this.supportedRange = supportedRange;
        }

        @Override
        protected boolean isValid(Integer value) {
            return supportedRange.contains(value);
        }
    }

    /// A [SingleIndexSettingValidator] for [OptionalInt]
    ///
    /// Valid: [OptionalInt#empty()] or inclusively within the provide range.
    public static final class OptionalIntRangeValidator extends SingleIndexSettingValidator<OptionalInt> {
        private final InclusiveRange<Integer> supportedRange;

        public static OptionalIntRangeValidator of(IndexSetting setting, int min, int max) {
            return new OptionalIntRangeValidator(setting, new InclusiveRange<>(min, max));
        }

        private OptionalIntRangeValidator(IndexSetting setting, InclusiveRange<Integer> supportedRange) {
            super(setting, OptionalInt.class, new DefaultRequirement<>(supportedRange));
            this.supportedRange = supportedRange;
        }

        @Override
        protected boolean isValid(OptionalInt value) {
            return value.isEmpty() || supportedRange.contains(value.getAsInt());
        }
    }

    /// A [SingleIndexSettingValidator] for [Double]
    ///
    /// Valid: non-null and inclusively within the provide range
    public static final class DoubleRangeValidator extends SingleIndexSettingValidator<Double> {
        private final InclusiveRange<Double> supportedRange;

        public static DoubleRangeValidator of(IndexSetting setting, double min, double max) {
            return new DoubleRangeValidator(setting, new InclusiveRange<>(min, max));
        }

        private DoubleRangeValidator(IndexSetting setting, InclusiveRange<Double> supportedRange) {
            super(setting, Double.class, new DefaultRequirement<>(supportedRange));
            this.supportedRange = supportedRange;
        }

        @Override
        protected boolean isValid(Double value) {
            return supportedRange.contains(value);
        }
    }
}
