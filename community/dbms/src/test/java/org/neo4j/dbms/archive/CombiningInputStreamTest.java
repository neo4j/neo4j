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
package org.neo4j.dbms.archive;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class CombiningInputStreamTest {

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    private static final int PART_SIZE = 100;

    @Test
    void shouldReadDataFromSinglePartArchive() throws IOException {
        byte[] expected = new byte[50];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldReadDataFromMultiplePartArchive() throws IOException {
        byte[] expected = new byte[250];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldReadRandomDataFromMultiplePartArchive() throws IOException {
        int parts = random.nextInt(100, 1000);
        byte[] expected = random.nextBytes(parts * PART_SIZE);

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldThrowWhenOnePartIsMissing() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[500]);
        Files.delete(firstPart.resolveSibling("test.dump.2"));

        try (CombiningInputStream in = open(firstPart)) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessageContaining("test.dump.2");
        }
    }

    @Test
    void shouldThrowWhenOnePartIsReplaced() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[500]);
        Files.delete(firstPart.resolveSibling("test.dump.2"));
        Files.write(firstPart.resolveSibling("test.dump.8"), new byte[0]);

        try (CombiningInputStream in = open(firstPart)) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessageContaining("test.dump.2");
        }
    }

    @Test
    void shouldThrowWhenDataMagicIsWrong() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[250]);
        Path secondPart = firstPart.resolveSibling("test.dump.1");
        fileSystem.renameFile(firstPart, secondPart.resolveSibling("test.dump.1.1"));
        // Note: By construction, we only select split archive if the first file
        // has the matching header.
        assertThatThrownBy(() -> open(secondPart).readAllBytes())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected format magic in split archive part:");
    }

    @Test
    void shouldThrowWhenFirstPartIsEmpty() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[250]);
        Files.write(firstPart, new byte[0]);
        assertThatThrownBy(() -> open(firstPart))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unexpected end of stream while reading metadata for split archive part:");
    }

    @Test
    void shouldThrowWhenMiddlePartIsEmpty() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[1000]);
        var indexEmptyFile = random.nextInt(1, 10);
        Files.write(firstPart.resolveSibling("test.dump." + indexEmptyFile), new byte[0]);

        try (CombiningInputStream in = open(firstPart)) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unexpected end of stream while reading metadata for split archive part:");
        }
    }

    @Test
    void shouldThrowWhenOnePartHasDifferentId() throws IOException {
        Path firstPart1 = writeSplitArchive("test.dump", new byte[250]);
        Path firstPart2 = writeSplitArchive("test1.dump", new byte[250]);

        fileSystem.renameFile(
                firstPart2.resolveSibling("test1.dump.1"), firstPart1.resolveSibling("test.dump.1"), REPLACE_EXISTING);

        try (CombiningInputStream in = open(firstPart1)) {
            assertThatThrownBy(in::readAllBytes)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Mismatching archive id in split archive part:");
        }
    }

    @Test
    void readSingleByteShouldReturnCorrectValues() throws IOException {
        // Spans three parts and includes bytes with the high bit set to verify read() returns 0..255, not negatives.
        byte[] expected = new byte[250];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }
        Path firstPart = writeSplitArchive("test.dump", expected);

        try (CombiningInputStream in = open(firstPart)) {
            for (byte b : expected) {
                assertThat(in.read()).isEqualTo(Byte.toUnsignedInt(b));
            }
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void readIntoBufferShouldReconstructDataAcrossParts() throws IOException {
        byte[] expected = random.nextBytes(250);
        Path firstPart = writeSplitArchive("test.dump", expected);

        byte[] actual = new byte[expected.length];
        try (CombiningInputStream in = open(firstPart)) {
            int offset = 0;
            int read;
            byte[] buffer = new byte[64];
            while ((read = in.read(buffer, 0, buffer.length)) != -1) {
                assertThat(read).isGreaterThan(0);
                System.arraycopy(buffer, 0, actual, offset, read);
                offset += read;
            }
            assertThat(offset).isEqualTo(expected.length);
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void readIntoBufferShouldOnlyTouchTheRequestedRange() throws IOException {
        byte[] expected = random.nextBytes(40);
        Path firstPart = writeSplitArchive("test.dump", expected);

        int offset = 10;
        int length = 20;
        byte[] buffer = new byte[50];

        try (CombiningInputStream in = open(firstPart)) {
            int read = in.read(buffer, offset, length);
            assertThat(read).isEqualTo(length);

            // Bytes outside [offset, offset + read) are left untouched.
            for (int i = 0; i < offset; i++) {
                assertThat(buffer[i]).isEqualTo((byte) 0);
            }
            for (int i = offset + read; i < buffer.length; i++) {
                assertThat(buffer[i]).isEqualTo((byte) 0);
            }
            assertThat(Arrays.copyOfRange(buffer, offset, offset + read)).isEqualTo(Arrays.copyOf(expected, read));
        }
    }

    @Test
    void readIntoBufferWithZeroLengthShouldReturnZero() throws IOException {
        byte[] expected = random.nextBytes(30);
        Path firstPart = writeSplitArchive("test.dump", expected);

        byte[] buffer = new byte[expected.length];
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.read(buffer, 0, 0)).isZero();
            // Nothing was consumed, so the data is still fully readable.
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void readIntoBufferWithInvalidRangeShouldThrow() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", random.nextBytes(30));

        byte[] buffer = new byte[10];
        try (CombiningInputStream in = open(firstPart)) {
            assertThatThrownBy(() -> in.read(buffer, -1, 5)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> in.read(buffer, 0, buffer.length + 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> in.read(buffer, 8, 5)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void transferToShouldWriteAllBytesAcrossParts() throws IOException {
        byte[] expected = random.nextBytes(250);
        Path firstPart = writeSplitArchive("test.dump", expected);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.transferTo(out)).isEqualTo(expected.length);
        }
        assertThat(out.toByteArray()).isEqualTo(expected);
    }

    @Test
    void transferToShouldOnlyWriteTheUnreadRemainder() throws IOException {
        byte[] expected = random.nextBytes(250);
        Path firstPart = writeSplitArchive("test.dump", expected);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (CombiningInputStream in = open(firstPart)) {
            out.write(in.read());
            byte[] chunk = new byte[120];
            int read = in.readNBytes(chunk, 0, chunk.length);
            out.write(chunk, 0, read);

            int alreadyRead = 1 + read;
            assertThat(in.transferTo(out)).isEqualTo(expected.length - alreadyRead);
        }
        assertThat(out.toByteArray()).isEqualTo(expected);
    }

    @Test
    void transferToOnExhaustedStreamShouldWriteNothing() throws IOException {
        byte[] expected = random.nextBytes(250);
        Path firstPart = writeSplitArchive("test.dump", expected);

        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertThat(in.transferTo(out)).isZero();
            assertThat(out.toByteArray()).isEmpty();
        }
    }

    @Test
    void shouldReadDataFromSinglePartArchiveUsingPartSupplier() throws IOException {
        byte[] expected = new byte[50];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldReadDataFromMultiplePartArchiveUsingPartSupplier() throws IOException {
        byte[] expected = new byte[250];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldReadRandomDataFromMultiplePartArchiveUsingPartSupplier() throws IOException {
        int parts = random.nextInt(100, 1000);
        byte[] expected = random.nextBytes(parts * PART_SIZE);

        Path firstPart = writeSplitArchive("test.dump", expected);
        try (CombiningInputStream in = open(firstPart)) {
            assertThat(in.readAllBytes()).isEqualTo(expected);
        }
    }

    @Test
    void shouldThrowWhenSupplierReturnsPartWithUnexpectedIndex() throws IOException {
        Path firstPart = writeSplitArchive("test.dump", new byte[250]); // test.dump, test.dump.1, test.dump.2

        // Swap the file names, to confuse the file index
        FileUtils.moveFile(firstPart.resolveSibling("test.dump.1"), firstPart.resolveSibling("tmp"));
        FileUtils.moveFile(firstPart.resolveSibling("test.dump.2"), firstPart.resolveSibling("test.dump.1"));
        FileUtils.moveFile(firstPart.resolveSibling("tmp"), firstPart.resolveSibling("test.dump.2"));

        var source = StreamSource.siblingsOf(fileSystem, firstPart);
        try (var metadataStream = source.next()) {
            metadataStream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
            try (var in = CombiningInputStream.of(metadataStream, source, "test")) {
                assertThatThrownBy(in::readAllBytes)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Unexpected part index in split archive");
            }
        }
    }

    private Path writeSplitArchive(String name, byte[] data) throws IOException {
        Path base = testDirectory.file(name);
        try (OutputStream out = new Dumper.SplitFileOutput(fileSystem, base, PART_SIZE).stream()) {
            out.write(data);
        }
        return base;
    }

    private CombiningInputStream open(Path firstPart) throws IOException {
        var source = StreamSource.siblingsOf(fileSystem, firstPart);
        var metadata = source.next();
        metadata.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH);
        return CombiningInputStream.of(metadata, source, "desc");
    }
}
