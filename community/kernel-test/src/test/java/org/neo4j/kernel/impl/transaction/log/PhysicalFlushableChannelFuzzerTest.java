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
package org.neo4j.kernel.impl.transaction.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.zip.Checksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class PhysicalFlushableChannelFuzzerTest {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @Test
    void shouldCalculateCorrectChecksum() throws IOException {
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);

        ArrayList<WrittenEntry> entries = new ArrayList<>();
        try (var channel = new PhysicalFlushableLogChannel(
                storeChannel, new HeapScopedBuffer(64, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {

            int numberOfEntries = random.intBetween(10, 100);
            for (int i = 0; i < numberOfEntries; i++) {
                channel.beginChecksumForWriting();
                int size = randomInteractions(channel);
                int channelChecksum = channel.putChecksum();
                entries.add(new WrittenEntry(size, channelChecksum));
            }
        }

        byte[] writtenBytes = Files.readAllBytes(file);
        ByteBuffer buffer = ByteBuffer.wrap(writtenBytes).order(ByteOrder.LITTLE_ENDIAN);

        int entryStart = 0;
        for (WrittenEntry entry : entries) {
            buffer.limit(entryStart + entry.size);
            buffer.position(entryStart);
            Checksum checksum = CHECKSUM_FACTORY.get();
            checksum.update(buffer);
            int bufferChecksum = (int) checksum.getValue();
            buffer.limit(buffer.limit() + Integer.BYTES);
            int writtenChecksum = buffer.getInt();
            assertEquals(writtenChecksum, entry.checksum);
            assertEquals(writtenChecksum, bufferChecksum);
            entryStart = buffer.limit();
        }
    }

    record WrittenEntry(int size, int checksum) {}

    private int randomInteractions(PhysicalFlushableLogChannel channel) throws IOException {
        int totalSize = 0;

        int numberOfInteractions = random.intBetween(1, 100);
        for (int i = 0; i < numberOfInteractions; i++) {
            switch (random.nextInt(9)) {
                case 0:
                    channel.put((byte) random.nextInt());
                    totalSize += Byte.BYTES;
                    break;
                case 1:
                    channel.putShort((short) random.nextInt());
                    totalSize += Short.BYTES;
                    break;
                case 2:
                    channel.putInt(random.nextInt());
                    totalSize += Integer.BYTES;
                    break;
                case 3:
                    channel.putFloat(random.nextFloat());
                    totalSize += Float.BYTES;
                    break;
                case 4:
                    channel.putDouble(random.nextDouble());
                    totalSize += Double.BYTES;
                    break;
                case 5:
                    byte[] bytes = randomBytes();
                    channel.put(bytes, bytes.length);
                    totalSize += bytes.length;
                    break;
                case 6:
                    bytes = randomBytes();
                    int offset = random.nextInt(30);
                    int sliceLength = bytes.length - offset - random.nextInt(30);
                    assert sliceLength > 0;
                    channel.put(bytes, offset, sliceLength);
                    totalSize += sliceLength;
                    break;
                case 7:
                    bytes = randomBytes();
                    channel.putAll(ByteBuffer.wrap(bytes));
                    totalSize += bytes.length;
                    break;
                case 8:
                    bytes = randomBytes();
                    ByteBuffer wrap = ByteBuffer.wrap(bytes);
                    wrap.limit(wrap.remaining() - random.intBetween(0, 30));
                    wrap.position(random.intBetween(0, 30));
                    totalSize += wrap.remaining();
                    channel.putAll(wrap);
                    break;
            }
        }

        return totalSize;
    }

    private byte[] randomBytes() {
        byte[] bytes = new byte[random.intBetween(64, 128)];
        random.nextBytes(bytes);
        return bytes;
    }
}
