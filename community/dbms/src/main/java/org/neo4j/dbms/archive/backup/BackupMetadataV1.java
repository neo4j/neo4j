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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.neo4j.io.fs.InputStreamReadableChannel;
import org.neo4j.io.fs.OutputStreamWritableChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.storageengine.api.StoreId;

public class BackupMetadataV1 implements BackupMetadata {
    private final String databaseName;
    private final StoreId storeId;
    private final DatabaseId databaseId;
    private final LocalDateTime backupTime;
    private final boolean recovered;
    private final boolean compressed;
    private final boolean full;
    private final long lowestAppendIndex;
    private final long highestAppendIndex;

    public static BackupMetadataV1 readFromStream(InputStream inputStream) throws IOException {
        return readMetadataV1(inputStream);
    }

    public BackupMetadataV1(BackupDescription description) {
        this(
                description.getDatabaseName(),
                description.getStoreId(),
                description.getDatabaseId(),
                description.getBackupTime(),
                description.getLowestAppendIndex(),
                description.getHighestAppendIndex(),
                description.isRecovered(),
                description.isCompressed(),
                description.isFull());
    }

    public BackupMetadataV1(
            String databaseName,
            StoreId storeId,
            DatabaseId databaseId,
            LocalDateTime backupTime,
            long lowestAppendIndex,
            long highestAppendIndex,
            boolean recovered,
            boolean compressed,
            boolean full) {
        this.databaseName = databaseName;
        this.storeId = storeId;
        this.databaseId = databaseId;
        this.backupTime = backupTime;
        this.recovered = recovered;
        this.compressed = compressed;
        this.full = full;
        this.lowestAppendIndex = lowestAppendIndex;
        this.highestAppendIndex = highestAppendIndex;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public StoreId getStoreId() {
        return storeId;
    }

    public LocalDateTime getBackupTime() {
        return backupTime;
    }

    public boolean isRecovered() {
        return recovered;
    }

    public boolean isFull() {
        return full;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public long getLowestAppendIndex() {
        return lowestAppendIndex;
    }

    public long getHighestAppendIndex() {
        return highestAppendIndex;
    }

    void writeToStreamV1(OutputStream compressionStream) throws IOException {
        writeStoreId(compressionStream, getStoreId());
        writeDatabaseId(compressionStream, getDatabaseId());
        writeString(compressionStream, getDatabaseName());
        writeLong(compressionStream, getBackupTime().toEpochSecond(UTC));
        writeLong(compressionStream, getLowestAppendIndex());
        writeLong(compressionStream, getHighestAppendIndex());

        BitSet flags = new BitSet(3);
        flags.set(0, isRecovered());
        flags.set(1, isCompressed());
        flags.set(2, isFull());
        writeBitSet(compressionStream, flags);
    }

    static BackupMetadataV1 readMetadataV1(InputStream inputStream) throws IOException {
        var storeId = readStoreId(inputStream);
        var databaseId = readDatabaseId(inputStream);
        var databaseName = readString(inputStream);
        var backupTime = LocalDateTime.ofEpochSecond(readLong(inputStream), 0, UTC);
        long lowestTransactionId = readLong(inputStream);
        long highestTransactionId = readLong(inputStream);
        BitSet flags = readBitSet(inputStream);
        return new BackupMetadataV1(
                databaseName,
                storeId,
                databaseId,
                backupTime,
                lowestTransactionId,
                highestTransactionId,
                flags.get(0),
                flags.get(1),
                flags.get(2));
    }

    static void writeLong(OutputStream compressionStream, long value) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.putLong(value);
        compressionStream.write(byteBuffer.array());
    }

    static long readLong(InputStream inputStream) throws IOException {
        return ByteBuffer.wrap(inputStream.readNBytes(Long.BYTES)).getLong();
    }

    private static void writeBitSet(OutputStream outputStream, BitSet bitSet) throws IOException {
        byte[] bitSetBytes = bitSet.toByteArray();
        outputStream.write(bitSetBytes.length);
        outputStream.write(bitSetBytes);
    }

    private static BitSet readBitSet(InputStream inputStream) throws IOException {
        int length = inputStream.read();
        byte[] data = inputStream.readNBytes(length);
        return BitSet.valueOf(data);
    }

    private static void writeString(OutputStream outputStream, String value) throws IOException {
        byte[] data = value.getBytes(UTF_8);
        outputStream.write(data.length);
        outputStream.write(data);
    }

    private static String readString(InputStream inputStream) throws IOException {
        return new String(inputStream.readNBytes(inputStream.read()), UTF_8);
    }

    private void writeStoreId(OutputStream compressionStream, StoreId storeId) throws IOException {
        storeId.serialize(new OutputStreamWritableChannel(compressionStream));
    }

    private static void writeDatabaseId(OutputStream compressionStream, DatabaseId databaseId) throws IOException {
        UUID uuid = databaseId.uuid();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES << 1);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        byte[] array = byteBuffer.array();
        compressionStream.write(array.length);
        compressionStream.write(array);
    }

    private static StoreId readStoreId(InputStream inputStream) throws IOException {
        return StoreId.deserialize(new InputStreamReadableChannel(inputStream));
    }

    private static DatabaseId readDatabaseId(InputStream inputStream) throws IOException {
        byte[] data = inputStream.readNBytes(inputStream.read());
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return DatabaseIdFactory.from(new UUID(buffer.getLong(), buffer.getLong()));
    }

    @Override
    public BackupDescription toBackupDescription() {
        return new BackupDescription(
                databaseName,
                storeId,
                databaseId,
                backupTime,
                recovered,
                compressed,
                full,
                lowestAppendIndex,
                highestAppendIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupMetadataV1 that = (BackupMetadataV1) o;
        return recovered == that.recovered
                && compressed == that.compressed
                && full == that.full
                && lowestAppendIndex == that.lowestAppendIndex
                && highestAppendIndex == that.highestAppendIndex
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(storeId, that.storeId)
                && Objects.equals(databaseId, that.databaseId)
                && Objects.equals(backupTime, that.backupTime);
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
                highestAppendIndex);
    }
}
