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
import java.util.Objects;
import java.util.Set;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigUtils.HasSetting;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/// An [IndexSettingsProcessor] that uses only a single setting's state
public abstract class SingleIndexSettingProcessor implements IndexSettingsProcessor, HasSetting {
    protected final IndexSetting setting;

    protected SingleIndexSettingProcessor(IndexSetting setting) {
        this.setting = setting;
    }

    /// Using the state from the provided record to produce a new record for further verification
    public abstract RecordWithSetting processForVerification(RecordWithSetting record);

    /// Using the state from the provided record to produce a new record
    public abstract RecordWithSetting processForAuthoritativeRead(RecordWithSetting record);

    @Override
    public void updateForVerification(KnownIndexSettingRecords records) {
        records.upsertWith(setting, this::processForVerification);
    }

    @Override
    public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
        records.upsertWith(setting, this::processForAuthoritativeRead);
    }

    @Override
    public IndexSetting setting() {
        return setting;
    }

    @Override
    public Set<IndexSetting> settings() {
        return Collections.singleton(setting);
    }

    @Override
    public String toString() {
        return "%s[%s]".formatted(getClass().getSimpleName(), setting);
    }

    // =================
    //  Implementations
    // =================

    public static final class MissingSettingMaterializer extends SingleIndexSettingProcessor {
        private final Object valueForAuthoritativeRead;
        private final Object valueForVerification;
        private final Value storableForVerification;

        public static MissingSettingMaterializer forVerification(IndexSetting setting, Object value) {
            return forVerification(setting, value, Values.unsafeOf(value, true));
        }

        public static MissingSettingMaterializer forVerification(
                IndexSetting setting, Object value, Value storableForVerification) {
            return of(setting, null, value, storableForVerification);
        }

        public static MissingSettingMaterializer of(
                IndexSetting setting,
                Object valueForAuthoritativeRead,
                Object valueForVerification,
                Value storableForVerification) {
            return new MissingSettingMaterializer(
                    setting, valueForAuthoritativeRead, valueForVerification, storableForVerification);
        }

        private MissingSettingMaterializer(
                IndexSetting setting,
                Object valueForAuthoritativeRead,
                Object valueForVerification,
                Value storableForVerification) {
            super(setting);
            this.valueForAuthoritativeRead = valueForAuthoritativeRead;
            this.valueForVerification = valueForVerification;
            this.storableForVerification = Objects.requireNonNullElse(storableForVerification, Values.NO_VALUE);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof MissingSetting missing)) {
                return record;
            }

            return new Pending(missing, valueForVerification, storableForVerification);
        }

        @Override
        public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
            if (valueForAuthoritativeRead == null || !(record instanceof MissingSetting missing)) {
                return record;
            }

            return new Valid(missing, valueForAuthoritativeRead, Values.NO_VALUE);
        }
    }

    public static final class RemoveSetting extends SingleIndexSettingProcessor {
        public static RemoveSetting of(IndexSetting setting) {
            return new RemoveSetting(setting);
        }

        private RemoveSetting(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof Valid valid)) {
                return record;
            }
            return new Valid(valid, null, Values.NO_VALUE);
        }

        @Override
        public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
            if (!(record instanceof Valid valid)) {
                return record;
            }
            return new Valid(valid, null, Values.NO_VALUE);
        }

        @Override
        public String toString() {
            return "Remove[%s]".formatted(setting);
        }
    }

    public static final class FinalizePending extends SingleIndexSettingProcessor
            implements ValidatingIndexSettingsProcessor {
        public static FinalizePending of(IndexSetting setting) {
            return new FinalizePending(setting);
        }

        private FinalizePending(IndexSetting setting) {
            super(setting);
        }

        @Override
        public RecordWithSetting processForVerification(RecordWithSetting record) {
            if (!(record instanceof Pending pending)) {
                return record;
            }

            return new Valid(pending);
        }

        @Override
        public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
            return record;
        }
    }
}
