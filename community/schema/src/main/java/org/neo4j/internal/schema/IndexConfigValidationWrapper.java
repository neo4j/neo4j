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

import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.INVALID_STATES;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.sorted.ImmutableSortedMap;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.sorted.ImmutableSortedSet;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.UnrecognizedSetting;
import org.neo4j.values.storable.Value;

public abstract class IndexConfigValidationWrapper {
    private final IndexProviderDescriptor descriptor;
    private final ImmutableSortedSet<String> validSettingNames;
    private final ImmutableSortedSet<String> possibleValidSettingNames;

    private final IndexConfig config;
    private final ImmutableSortedMap<IndexSetting, Object> settings;

    protected IndexConfigValidationWrapper(
            IndexProviderDescriptor descriptor,
            IndexConfig config,
            ImmutableSortedMap<IndexSetting, Object> settings,
            ImmutableSortedSet<String> validSettingNames,
            ImmutableSortedSet<String> possibleValidSettingNames) {
        this.descriptor = descriptor;
        this.validSettingNames = validSettingNames;
        this.possibleValidSettingNames = possibleValidSettingNames;

        this.config = validateSettingNames(config);
        this.settings = validatePossibleSettingNames(settings);
    }

    public IndexProviderDescriptor descriptor() {
        return descriptor;
    }

    public IndexConfig config() {
        return config;
    }

    public <T extends Value> T getValue(String setting) {
        if (!validSettingNames.contains(setting)) {
            throw unrecognizedSetting(setting, descriptor, validSettingNames);
        }
        return config.get(setting);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(IndexSetting setting) {
        final var settingName = setting.getSettingName();
        if (!possibleValidSettingNames.contains(settingName)) {
            throw unrecognizedSetting(settingName, descriptor, possibleValidSettingNames);
        }
        return (T) settings.get(setting);
    }

    public static IndexConfigValidationRecords validateSettingNames(
            SetIterable<String> settingNames, SetIterable<String> validSettingNames) {
        final var validationRecords = new IndexConfigValidationRecords();
        settingNames
                .differenceInto(validSettingNames, Sets.mutable.empty())
                .asLazy()
                .collect(UnrecognizedSetting::new)
                .forEach(validationRecords::with);
        return validationRecords;
    }

    private IndexConfig validateSettingNames(IndexConfig config) {
        final var settingNames = config.entries().asLazy().collect(Pair::getOne).toSet();
        assertValidSettingNames(validateSettingNames(settingNames, validSettingNames), validSettingNames);
        return config;
    }

    private ImmutableSortedMap<IndexSetting, Object> validatePossibleSettingNames(
            ImmutableSortedMap<IndexSetting, Object> settings) {
        final var settingNames = settings.keysView()
                .asLazy()
                .collect(IndexSetting::getSettingName)
                .toSet();

        assertValidSettingNames(
                validateSettingNames(settingNames, possibleValidSettingNames), possibleValidSettingNames);
        return settings;
    }

    private void assertValidSettingNames(
            IndexConfigValidationRecords validationRecords, Iterable<String> validSettingNames) {
        if (validationRecords.valid()) {
            return;
        }

        // fail on first
        final var invalidRecord =
                INVALID_STATES.asLazy().flatCollect(validationRecords::get).getFirst();
        throw unrecognizedSetting(invalidRecord.settingName(), descriptor, validSettingNames);
    }

    public static IllegalArgumentException unrecognizedSetting(
            String settingName, IndexProviderDescriptor descriptor, Iterable<String> validSettingNames) {
        return new IllegalArgumentException("'%s' is an unrecognized setting for index with provider '%s'. "
                        .formatted(settingName, descriptor.name())
                + "Supported: " + validSettingNames);
    }
}
