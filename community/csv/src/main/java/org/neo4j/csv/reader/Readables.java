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
package org.neo4j.csv.reader;

import static org.neo4j.csv.reader.BufferedCharSeeker.isEolChar;
import static org.neo4j.csv.reader.CharReadable.EMPTY;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.neo4j.collection.RawIterator;
import org.neo4j.function.IOFunction;
import org.neo4j.function.ThrowingFunction;

/**
 * Means of instantiating common {@link CharReadable} instances.
 * <br>
 * There are support for compressed files as well for those methods accepting a {@link Path} argument.
 * <ol>
 * <li>ZIP: is both an archive and a compression format. In many cases the order of files
 * is important and for a ZIP archive with multiple files, the order of the files are whatever the order
 * set by the tool that created the ZIP archive. Therefore only single-file-zip files are supported.
 * The single file in the given ZIP archive will be decompressed on the fly, while reading.</li>
 * <li>GZIP: is only a compression format and so will be decompressed on the fly, while reading.</li>
 * </ol>
 */
public class Readables {
    private Readables() {
        throw new AssertionError("No instances allowed");
    }

    public static CharReadable wrap(final InputStream stream, final String sourceName, Charset charset)
            throws IOException {
        return wrap(stream, sourceName, charset, 0);
    }

    /**
     * Wraps a {@link InputStream} in a {@link CharReadable}.
     *
     * @param stream {@link Reader} to wrap.
     * @param sourceName name or description of the source of the stream.
     * @param charset {@link Charset} to use for reading.
     * @param length total number of bytes provided by the reader.
     * @return a {@link CharReadable} for the {@link Reader}.
     * @throws IOException on I/O error.
     */
    public static CharReadable wrap(final InputStream stream, final String sourceName, Charset charset, long length)
            throws IOException {
        byte[] bytes = new byte[Magic.longest()];
        PushbackInputStream pushbackStream = new PushbackInputStream(stream, bytes.length);
        Charset usedCharset = charset;
        int read = stream.read(bytes);
        if (read >= 0) {
            bytes = read < bytes.length ? Arrays.copyOf(bytes, read) : bytes;
            Magic magic = Magic.of(bytes);
            int excessiveBytes = read;
            if (magic.impliesEncoding()) {
                // Unread the diff between the BOM and the longest magic we gathered bytes for
                excessiveBytes -= magic.length();
                usedCharset = magic.encoding();
            }
            pushbackStream.unread(bytes, read - excessiveBytes, excessiveBytes);
        }
        return wrap(
                new InputStreamReader(pushbackStream, usedCharset) {
                    @Override
                    public String toString() {
                        return sourceName;
                    }
                },
                length);
    }

    public static CharReadable wrap(String sourceDescription, String data) {
        return wrap(sourceDescription, new StringReader(data), data.length());
    }

    public static CharReadable wrap(String data) {
        return wrap(new StringReader(data), data.length());
    }

    /**
     * Wraps a {@link Reader} in a {@link CharReadable}.
     * Remember that the {@link Reader#toString()} must provide a description of the data source.
     * Uses {@link Reader#toString()} as the source description.
     *
     * @param reader {@link Reader} to wrap.
     * @param length total number of bytes provided by the reader.
     * @return a {@link CharReadable} for the {@link Reader}.
     */
    public static CharReadable wrap(Reader reader, long length) {
        return wrap(reader.toString(), reader, length);
    }

    /**
     * Wraps a {@link Reader} in a {@link CharReadable}.
     * Remember that the {@link Reader#toString()} must provide a description of the data source.
     *
     * @param reader {@link Reader} to wrap.
     * @param length total number of bytes provided by the reader.
     * @return a {@link CharReadable} for the {@link Reader}.
     */
    public static CharReadable wrap(String sourceDescription, Reader reader, long length) {
        return new WrappedCharReadable(length, reader, sourceDescription);
    }

    private record FromFile(Charset charset) implements IOFunction<Path, CharReadable> {
        @Override
        public CharReadable apply(final Path path) throws IOException {
            final var input = MagicInputStream.create(path);
            if (input.magic() == Magic.ZIP) {
                return input.isDefaultFileSystemBased()
                        ? zipReadableFromFile(input.path(), charset)
                        : zipReadable(input, charset);
            } else if (input.magic() == Magic.GZIP) {
                return gzipReadable(input, charset);
            } else {
                return readableWithEncoding(input, charset);
            }
        }
    }

    private static CharReadable zipReadable(MagicInputStream input, Charset charset) throws IOException {
        // can't use ZipFile unfortunately as the (storage) Path implementation would throw on a toFile call :-(
        final var stream = new ZipInputStream(input);

        ZipEntry entry;
        while ((entry = stream.getNextEntry()) != null) {
            if (entry.isDirectory() || invalidZipEntry(entry.getName())) {
                continue;
            }

            return wrap(
                    new InputStreamReader(stream, charset) {
                        @Override
                        public String toString() {
                            return input.path().toAbsolutePath().toString();
                        }
                    },
                    entry.getSize());
        }

        stream.close();
        throw new IllegalStateException("Couldn't find zip entry when opening the stream at " + input.path());
    }

    private static CharReadable zipReadableFromFile(Path path, Charset charset) throws IOException {
        try (var zipFile = new ZipFile(path.toFile())) {
            final var entry = getSingleSuitableEntry(zipFile);
            return wrap(
                    new InputStreamReader(openZipInputStream(path, entry), charset) {
                        @Override
                        public String toString() {
                            return path.toAbsolutePath().toString();
                        }
                    },
                    entry.getSize());
        }
    }

    private static CharReadable gzipReadable(MagicInputStream input, Charset charset) throws IOException {
        final var path = input.path();
        // GZIP isn't an archive like ZIP, so this is purely data that is compressed.
        // Although a very common way of compressing with GZIP is to use TAR which can combine many files into one
        // blob, which is then compressed. If that's the case then the data will look like garbage and the reader
        // will fail for whatever it will be used for.
        // TODO add tar support
        LongSupplier[] bytesReadFromCompressedSource = new LongSupplier[1];
        GZIPInputStream zipStream = new GZIPInputStream(input) {
            {
                // Access GZIPInputStream's internal Inflater instance and make number of bytes read available
                // to the returned CharReadable below.
                bytesReadFromCompressedSource[0] = inf::getBytesRead;
            }
        };
        InputStreamReader reader = new InputStreamReader(zipStream, charset) {
            @Override
            public String toString() {
                return path.toAbsolutePath().toString();
            }
        };
        // For GZIP there's no reliable way of getting the decompressed file size w/o decompressing the whole file,
        // therefore this compression ratio estimation mechanic is put in place such that at any given time the
        // reader can be queried about its observed compression ratio and the longer the reader goes the more
        // accurate it gets.
        return new WrappedCharReadable(
                Files.size(path), reader, path.toAbsolutePath().toString()) {
            @Override
            public float compressionRatio() {
                long decompressedPosition = position();
                long compressedPosition = bytesReadFromCompressedSource[0].getAsLong();
                return (float) ((double) compressedPosition / decompressedPosition);
            }
        };
    }

    private static CharReadable readableWithEncoding(MagicInputStream input, Charset defaultCharset)
            throws IOException {
        final var magic = input.magic();
        var usedCharset = defaultCharset;
        if (magic.impliesEncoding()) {
            // Read (and skip) the magic (BOM in this case) from the file we're returning out
            long skip = input.skip(magic.length());
            if (skip != magic.length()) {
                throw new IOException(
                        "Unable to skip " + magic.length() + " bytes, only able to skip " + skip + " bytes.");
            }
            usedCharset = magic.encoding();
        }
        return wrap(
                new InputStreamReader(input, usedCharset) {
                    @Override
                    public String toString() {
                        return input.path().toAbsolutePath().toString();
                    }
                },
                Files.size(input.path()));
    }

    private static ZipInputStream openZipInputStream(Path path, ZipEntry entry) throws IOException {
        var stream = new ZipInputStream(new BufferedInputStream(new FileInputStream(path.toFile())));
        ZipEntry readEntry;
        while ((readEntry = stream.getNextEntry()) != null) {
            if (!readEntry.isDirectory() && readEntry.getName().equals(entry.getName())) {
                return stream;
            }
        }
        stream.close();
        throw new IllegalStateException(
                "Couldn't find zip entry with name " + entry.getName() + " when opening it as a stream");
    }

    private static ZipEntry getSingleSuitableEntry(ZipFile zipFile) throws IOException {
        List<String> unsuitableEntries = new ArrayList<>();
        Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
        ZipEntry found = null;
        while (enumeration.hasMoreElements()) {
            ZipEntry entry = enumeration.nextElement();
            if (entry.isDirectory() || invalidZipEntry(entry.getName())) {
                unsuitableEntries.add(entry.getName());
                continue;
            }

            if (found != null) {
                throw new IOException("Multiple suitable files found in zip file " + zipFile.getName() + ", at least "
                        + found.getName() + " and " + entry.getName()
                        + ". Only a single file per zip file is supported");
            }
            found = entry;
        }

        if (found == null) {
            throw new IOException("No suitable file found in zip file " + zipFile.getName() + "."
                    + (!unsuitableEntries.isEmpty()
                            ? " Although found these unsuitable entries " + unsuitableEntries
                            : ""));
        }
        return found;
    }

    private static boolean invalidZipEntry(String name) {
        return name.contains("__MACOSX") || name.startsWith(".") || name.contains("/.");
    }

    public static RawIterator<CharReadable, IOException> individualFiles(Charset charset, Path... files) {
        return iterator(new FromFile(charset), files);
    }

    public static CharReadable files(Charset charset, Path... files) throws IOException {
        IOFunction<Path, CharReadable> opener = new FromFile(charset);
        return switch (files.length) {
            case 0 -> EMPTY;
            case 1 -> opener.apply(files[0]);
            default -> new MultiReadable(iterator(opener, files));
        };
    }

    @SafeVarargs
    public static <IN, OUT> RawIterator<OUT, IOException> iterator(
            ThrowingFunction<IN, OUT, IOException> converter, IN... items) {
        if (items.length == 0) {
            throw new IllegalStateException("No source items specified");
        }

        return new RawIterator<>() {
            private int cursor;

            @Override
            public boolean hasNext() {
                return cursor < items.length;
            }

            @Override
            public OUT next() throws IOException {
                if (!hasNext()) {
                    throw new IllegalStateException();
                }
                return converter.apply(items[cursor++]);
            }
        };
    }

    public static char[] extractFirstLineFrom(CharReadable source) throws IOException {
        return extractFirstLineFrom((into, offset) -> source.read(into, offset, 1) > 0);
    }

    public static char[] extractFirstLineFrom(char[] data, int offset, int length) {
        try {
            return extractFirstLineFrom((into, intoOffset) -> {
                if (intoOffset < length) {
                    into[intoOffset] = data[offset + intoOffset];
                    return true;
                }
                return false;
            });
        } catch (IOException e) {
            // Will not happen because we're reading from an array
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Extracts the first line, i.e characters until the first newline or end of stream.
     * Reads one character at a time to be sure not to read too far ahead. The stream is left
     * in a state of either exhausted or at the beginning of the next line of data.
     *
     * @param source {@link CharReadable} to read from.
     * @return char[] containing characters until the first newline character or end of stream.
     * @throws IOException on I/O reading error.
     */
    public static char[] extractFirstLineFrom(CharSupplier source) throws IOException {
        char[] result = new char[100];
        int cursor = 0;
        boolean foundEol;
        do {
            // Grow on demand
            if (cursor >= result.length) {
                result = Arrays.copyOf(result, cursor * 2);
            }

            // Read one character
            if (!source.next(result, cursor)) {
                break;
            }
            foundEol = isEolChar(result[cursor]);
            if (!foundEol) {
                cursor++;
            }
        } while (!foundEol);
        return Arrays.copyOf(result, cursor);
    }

    public interface CharSupplier {
        boolean next(char[] into, int offset) throws IOException;
    }
}
