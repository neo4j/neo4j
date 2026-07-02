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
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsRequirements.ClassRequirement;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public abstract class MissingDependentSettingMaterializer<DEPENDENCY, TYPE> implements IndexSettingsProcessor {
    private final IndexSetting dependencySetting;
    private final IndexSetting setting;
    private final Class<TYPE> type;

    protected MissingDependentSettingMaterializer(
            IndexSetting dependencySetting, IndexSetting setting, Class<TYPE> type) {
        this.dependencySetting = dependencySetting;
        this.setting = setting;
        this.type = type;
    }

    protected abstract TYPE dependentValueForVerification(DEPENDENCY dependency);

    protected TYPE dependentValueForAuthoritativeRead(DEPENDENCY dependency) {
        return null;
    }

    protected Value toStorable(TYPE value) {
        return Values.unsafeOf(value, true);
    }

    @Override
    public void updateForVerification(KnownIndexSettingRecords records) {
        if (!(records.get(setting) instanceof MissingSetting missing)) {
            return;
        }
        if (!(records.get(dependencySetting) instanceof Valid validType)) {
            records.upsert(new InvalidValue(missing, null, new ClassRequirement(type)));
            return;
        }

        TYPE value = dependentValueForVerification(validType.get());
        records.upsert(new Pending(missing, value, toStorable(value)));
    }

    @Override
    public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
        if (!(records.get(setting) instanceof MissingSetting missing)) {
            return;
        }

        Valid validType = (Valid) records.get(dependencySetting);
        TYPE value = dependentValueForAuthoritativeRead(validType.get());
        records.upsert(new Valid(missing, value, toStorable(value)));
    }

    @Override
    public Set<IndexSetting> settings() {
        return Set.of(dependencySetting, setting);
    }

    @Override
    public String toString() {
        return Iterables.toString(settings(), ", ", getClass().getSimpleName() + "[", "]");
    }
}
