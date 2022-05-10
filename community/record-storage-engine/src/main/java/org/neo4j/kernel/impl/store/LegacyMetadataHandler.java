/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;

/**
 * Store format version used to be represented by a single long.
 * Since the current binaries need to be able to understand pre 5.0 metadata
 * like metadata store or transaction logs, code that can transform
 * the old format version into the new format is still needed and this is the place where it lives.
 * TODO: More methods related to the old store version format will be moved here.
 */
public class LegacyMetadataHandler {

    public static StoreId storeIdFromLegacyMetadata(long creationTime, long random, long legacyVersion) {
        String versionString = StoreVersion.versionLongToString(legacyVersion);
        RecordFormats recordFormat = RecordFormatSelector.selectForVersion(versionString);
        return new StoreId(
                creationTime,
                random,
                RecordStorageEngineFactory.NAME,
                recordFormat.getFormatFamily().name(),
                recordFormat.majorVersion(),
                recordFormat.minorVersion());
    }
}
