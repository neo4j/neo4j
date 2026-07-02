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

import java.util.Set;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsRequirements.ClassRequirement;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/// A [SingleIndexSettingProcessor] that transforms a valid value from one [IndexSetting] to another
public abstract class SingleIndexSettingMigrator<FROM, TO> extends SingleIndexSettingProcessor {
    protected final IndexSetting fromSetting;
    protected final Class<FROM> fromType;
    protected final IndexSetting toSetting;
    protected final Class<TO> toType;

    protected SingleIndexSettingMigrator(
            IndexSetting fromSetting, Class<FROM> fromType, IndexSetting toSetting, Class<TO> toType) {
        super(fromSetting);
        this.fromSetting = fromSetting;
        this.fromType = fromType;
        this.toSetting = toSetting;
        this.toType = toType;
    }

    protected abstract TO migrate(FROM value);

    protected Value toStorable(TO value) {
        return Values.unsafeOf(value, true);
    }

    /// Migrates the value from one [Valid] record to a [Pending] record of a new setting using [#migrate(FROM)].
    /// If a non-valid [RecordWithSetting] is provided an [InvalidValue] will be returned.
    @Override
    public RecordWithSetting processForVerification(RecordWithSetting record) {
        if (!(record instanceof Valid valid && fromType.isInstance(valid.value()))) {
            return new InvalidValue(toSetting, null, new ClassRequirement(toType));
        }

        TO value = migrate(valid.valueAs(fromType));
        return new Pending(toSetting, value, toStorable(value));
    }

    @Override
    public RecordWithSetting processForAuthoritativeRead(RecordWithSetting record) {
        if (!(record instanceof Valid valid)) {
            return new MissingSetting(toSetting);
        }

        TO value = migrate(valid.valueAs(fromType));
        return new Valid(toSetting, migrate(valid.valueAs(fromType)), toStorable(value));
    }

    @Override
    public void updateForVerification(KnownIndexSettingRecords records) {
        // override not needed, but skips superfluous logic
        records.upsert(processForVerification(records.get(fromSetting)));
    }

    @Override
    public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
        // override not needed, but skips superfluous logic
        records.upsert(processForAuthoritativeRead(records.get(fromSetting)));
    }

    @Override
    public Set<IndexSetting> settings() {
        return Set.of(fromSetting, toSetting);
    }
}
