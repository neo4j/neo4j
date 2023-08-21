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
package org.neo4j.kernel.impl.storemigration;

import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionUserStringProvider;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public class RecordStoreVersion implements StoreVersion {
    private final RecordFormats format;

    public RecordStoreVersion(RecordFormats format) {
        this.format = format;
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return format.hasCapability(capability);
    }

    @Override
    public boolean hasCompatibleCapabilities(StoreVersion otherVersion, CapabilityType type) {
        if (otherVersion instanceof RecordStoreVersion) {
            return format.hasCompatibleCapabilities(((RecordStoreVersion) otherVersion).format, type);
        }

        return false;
    }

    @Override
    public String introductionNeo4jVersion() {
        return format.introductionVersion();
    }

    @Override
    public Optional<StoreVersion> successorStoreVersion(Config config) {
        RecordFormats latestFormatInFamily = RecordFormatSelector.findLatestFormatInFamily(
                format.getFormatFamily().name(), config);
        if (!latestFormatInFamily.name().equals(format.name())) {
            return Optional.of(new RecordStoreVersion(latestFormatInFamily));
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return format.getFormatFamily().name();
    }

    @Override
    public boolean onlyForMigration() {
        return format.onlyForMigration();
    }

    public RecordFormats getFormat() {
        return format;
    }

    @Override
    public String toString() {
        return "RecordStoreVersion{" + "format=" + format + '}';
    }

    @Override
    public String getStoreVersionUserString() {
        return StoreVersionUserStringProvider.formatVersion(
                RecordStorageEngineFactory.NAME, formatName(), format.majorVersion(), format.minorVersion());
    }
}
