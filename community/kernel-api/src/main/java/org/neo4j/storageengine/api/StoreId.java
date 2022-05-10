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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.util.Objects;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * A representation of store ID.
 * <p>
 * TODO: The aim is to have this as the only representation of store ID and store version
 * and get rid of the 'String' and 'long' representation of store version and {@link LegacyStoreId}.
 */
public class StoreId extends StoreVersionIdentifier {
    private final long creationTime;
    private final long random;

    public StoreId(
            long creationTime,
            long random,
            String storageEngineName,
            String formatFamilyName,
            int majorVersion,
            int minorVersion) {
        super(storageEngineName, formatFamilyName, majorVersion, minorVersion);
        this.creationTime = creationTime;
        this.random = random;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getRandom() {
        return random;
    }

    public void serialize(WritableChannel channel) throws IOException {
        StoreIdSerialization.serialize(this, channel);
    }

    /**
     * Returns {@code true} if the submitted {@code anotherId} is an upgrade successor of this store ID.
     * This means that the IDs have all the same attributes, but possibly differ only in the minor store version part
     * and the minor store version of the submitted {@code anotherId} is greater than or equal to the minor version
     * of this store version.
     */
    public boolean isSameOrUpgradeSuccessor(StoreId anotherId) {
        return creationTime == anotherId.creationTime
                && random == anotherId.random
                && super.isSameOrUpgradeSuccessor(anotherId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreId storeId = (StoreId) o;
        return creationTime == storeId.creationTime
                && random == storeId.random
                && getMajorVersion() == storeId.getMajorVersion()
                && getMinorVersion() == storeId.getMinorVersion()
                && getStorageEngineName().equals(storeId.getStorageEngineName())
                && getFormatFamilyName().equals(storeId.getFormatFamilyName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                creationTime,
                random,
                getStorageEngineName(),
                getFormatFamilyName(),
                getMajorVersion(),
                getMinorVersion());
    }

    @Override
    public String toString() {
        return "StoreId{" + "creationTime="
                + creationTime + ", random="
                + random + ", storageEngineName='"
                + getStorageEngineName() + '\'' + ", formatFamilyName='"
                + getFormatFamilyName() + '\'' + ", majorVersion="
                + getMajorVersion() + ", minorVersion="
                + getMinorVersion() + '}';
    }

    public static StoreId deserialize(ReadableChannel channel) throws IOException {
        return StoreIdSerialization.deserialize(channel);
    }

    /**
     * Retrieves ID of the store represented by the submitted layout.
     * <p>
     * This method will return {@code null} if the layout points to an empty or uninitialised store.
     */
    public static StoreId retrieveFromStore(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext)
            throws IOException {
        var maybeEngine = StorageEngineFactory.selectStorageEngine(fs, databaseLayout, pageCache);
        if (maybeEngine.isEmpty()) {
            return null;
        }

        return maybeEngine.get().retrieveStoreId(fs, databaseLayout, pageCache, cursorContext);
    }
}
