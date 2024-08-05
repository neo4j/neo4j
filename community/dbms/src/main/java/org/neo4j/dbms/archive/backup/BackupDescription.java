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
package org.neo4j.dbms.archive.backup;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.storageengine.api.StoreId;

public class BackupDescription {
    private final String databaseName;
    private final StoreId storeId;
    private final DatabaseId databaseId;
    private final LocalDateTime backupTime;
    private final boolean recovered;
    private final boolean compressed;
    private final boolean full;
    private final long lowestAppendIndex;
    private final long highestAppendIndex;
    private final String metadataScript;

    public BackupDescription(
            String databaseName,
            StoreId storeId,
            DatabaseId databaseId,
            LocalDateTime backupTime,
            boolean recovered,
            boolean compressed,
            boolean full,
            long lowestAppendIndex,
            long highestAppendIndex) {
        this.databaseName = new NormalizedDatabaseName(databaseName).name();
        this.storeId = storeId;
        this.databaseId = databaseId;
        this.backupTime = backupTime.truncatedTo(ChronoUnit.SECONDS);
        this.recovered = recovered;
        this.compressed = compressed;
        this.full = full;
        this.lowestAppendIndex = lowestAppendIndex;
        this.highestAppendIndex = highestAppendIndex;
        this.metadataScript = null;
    }

    private BackupDescription(
            String databaseName,
            StoreId storeId,
            DatabaseId databaseId,
            LocalDateTime backupTime,
            boolean recovered,
            boolean compressed,
            boolean full,
            long lowestAppendIndex,
            long highestAppendIndex,
            String metadataScript) {
        this.databaseName = new NormalizedDatabaseName(databaseName).name();
        this.storeId = storeId;
        this.databaseId = databaseId;
        this.backupTime = backupTime.truncatedTo(ChronoUnit.SECONDS);
        this.recovered = recovered;
        this.compressed = compressed;
        this.full = full;
        this.lowestAppendIndex = lowestAppendIndex;
        this.highestAppendIndex = highestAppendIndex;
        this.metadataScript = metadataScript;
    }

    public BackupDescription withMetadataScript(String metadataScript) {
        return new BackupDescription(
                databaseName,
                storeId,
                databaseId,
                backupTime,
                recovered,
                compressed,
                full,
                lowestAppendIndex,
                highestAppendIndex,
                metadataScript);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public LocalDateTime getBackupTime() {
        return backupTime;
    }

    public boolean isRecovered() {
        return recovered;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public StoreId getStoreId() {
        return storeId;
    }

    public boolean isFull() {
        return full;
    }

    public long getLowestAppendIndex() {
        return lowestAppendIndex;
    }

    public long getHighestAppendIndex() {
        return highestAppendIndex;
    }

    public String getMetadataScript() {
        return metadataScript;
    }

    public boolean isEmpty() {
        return lowestAppendIndex == 0 && highestAppendIndex == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BackupDescription that = (BackupDescription) o;
        return recovered == that.recovered
                && compressed == that.compressed
                && full == that.full
                && lowestAppendIndex == that.lowestAppendIndex
                && highestAppendIndex == that.highestAppendIndex
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(storeId, that.storeId)
                && Objects.equals(databaseId, that.databaseId)
                && Objects.equals(backupTime, that.backupTime)
                && Objects.equals(metadataScript, that.metadataScript);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                databaseName,
                storeId,
                databaseId,
                backupTime,
                recovered,
                compressed,
                full,
                lowestAppendIndex,
                highestAppendIndex,
                metadataScript);
    }

    @Override
    public String toString() {
        return "BackupDescription{" + "databaseName='"
                + databaseName + '\'' + ", storeId="
                + storeId + ", databaseId="
                + databaseId + ", backupTime="
                + backupTime + ", recovered="
                + recovered + ", compressed="
                + compressed + ", full="
                + full + ", lowestAppendIndex="
                + lowestAppendIndex + ", highestAppendIndex="
                + highestAppendIndex + ", metadataScript='"
                + metadataScript + '\'' + '}';
    }
}
