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
package org.neo4j.kernel.impl.store.format;

import static java.util.stream.Collectors.toSet;
import static org.neo4j.internal.helpers.ArrayUtil.contains;

import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.NoRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.StoreFormatLimits;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * Base class for simpler implementation of {@link RecordFormats}.
 */
public abstract class BaseRecordFormats implements RecordFormats {
    private final int majorFormatVersion;
    private final int minorFormatVersion;
    private final boolean onlyForMigration;
    private final Capability[] capabilities;
    private final String introductionVersion;

    protected BaseRecordFormats(StoreVersion storeVersion, Capability... capabilities) {
        this.onlyForMigration = storeVersion.onlyForMigration();
        this.majorFormatVersion = storeVersion.majorVersion();
        this.minorFormatVersion = storeVersion.minorVersion();
        this.capabilities = capabilities;
        this.introductionVersion = storeVersion.introductionVersion();
    }

    @Override
    public String introductionVersion() {
        return introductionVersion;
    }

    @Override
    public int majorVersion() {
        return majorFormatVersion;
    }

    @Override
    public int minorVersion() {
        return minorFormatVersion;
    }

    @Override
    public RecordFormat<MetaDataRecord> metaData() {
        return new MetaDataRecordFormat();
    }

    @Override
    public boolean onlyForMigration() {
        return onlyForMigration;
    }

    @Override
    public String toString() {
        return String.format(
                "RecordFormat:%s[%s-%d.%d]",
                getClass().getSimpleName(), getFormatFamily().name(), majorFormatVersion, minorFormatVersion);
    }

    @Override
    public Capability[] capabilities() {
        return capabilities;
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return contains(capabilities(), capability);
    }

    public static boolean hasCompatibleCapabilities(RecordFormats one, RecordFormats other, CapabilityType type) {
        Set<Capability> myFormatCapabilities = Stream.of(one.capabilities())
                .filter(capability -> capability.isType(type))
                .collect(toSet());
        Set<Capability> otherFormatCapabilities = Stream.of(other.capabilities())
                .filter(capability -> capability.isType(type))
                .collect(toSet());

        if (myFormatCapabilities.equals(otherFormatCapabilities)) {
            // If they have the same capabilities then of course they are compatible
            return true;
        }

        boolean capabilitiesNotRemoved = otherFormatCapabilities.containsAll(myFormatCapabilities);

        otherFormatCapabilities.removeAll(myFormatCapabilities);
        boolean allAddedAreAdditive = otherFormatCapabilities.stream().allMatch(Capability::isAdditive);

        // Even if capabilities of the two aren't the same then there's a special case where if the additional
        // capabilities of the other format are all additive then they are also compatible because no data
        // in the existing store needs to be migrated.
        return capabilitiesNotRemoved && allAddedAreAdditive;
    }

    @Override
    public boolean hasCompatibleCapabilities(RecordFormats other, CapabilityType type) {
        return hasCompatibleCapabilities(this, other, type);
    }

    @Override
    public RecordFormat<SchemaRecord> schema() {
        return new NoRecordFormat<>();
    }

    @Override
    public StoreFormatLimits idLimits() {
        return StandardFormatSettings.LIMITS;
    }
}
