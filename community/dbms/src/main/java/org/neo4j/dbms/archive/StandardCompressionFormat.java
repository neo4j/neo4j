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

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.github.luben.zstd.util.Native;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.IOUtils;

public enum StandardCompressionFormat implements CompressionFormat {
    GZIP {
        @Override
        public OutputStream compress(OutputStream stream) throws IOException {
            return new GZIPOutputStream(stream);
        }

        @Override
        public InputStream decompress(InputStream stream) throws IOException {
            return new GZIPInputStream(stream);
        }
    },
    ZSTD {
        // ZSTD does not check a magic header on initialisation, like GZIP does, so we have to do that ourselves.
        // We use this header for that purpose.
        private final byte[] HEADER = new byte[] {'z', 's', 't', 'd'};

        @Override
        public OutputStream compress(OutputStream stream) throws IOException {
            var zstdout = new ZstdOutputStreamNoFinalizer(stream);
            try {
                zstdout.setChecksum(true);
                if (Runtime.getRuntime().availableProcessors() > 2) {
                    zstdout.setWorkers(Runtime.getRuntime().availableProcessors());
                }
                zstdout.write(HEADER);
                return zstdout;
            } catch (IOException e) {
                zstdout.close();
                throw e;
            }
        }

        @Override
        public InputStream decompress(InputStream stream) throws IOException {
            var zstdin = new ZstdInputStreamNoFinalizer(stream);
            try {
                byte[] header = new byte[HEADER.length];
                if (zstdin.read(header) != HEADER.length || !Arrays.equals(header, HEADER)) {
                    throw new IOException("Not in ZSTD format");
                }
                return zstdin;
            } catch (IOException e) {
                zstdin.close();
                throw e;
            }
        }
    };

    /**
     * @return {@code true} if the given {@link InputStream} is <em>directly</em> a compressed input stream of this format. With "directly" meaning that the
     * compressed stream is not wrapped in other streams, like buffered or filtering input streams.
     */
    public boolean isFormat(InputStream stream) {
        return (this == ZSTD && stream instanceof ZstdInputStreamNoFinalizer)
                || (this == GZIP && stream instanceof GZIPInputStream);
    }

    public boolean isFormat(OutputStream stream) {
        return (this == ZSTD && stream instanceof ZstdOutputStreamNoFinalizer)
                || (this == GZIP && stream instanceof GZIPOutputStream);
    }

    public static OutputStream compress(
            ThrowingSupplier<OutputStream, IOException> streamSupplier, CompressionFormat format) throws IOException {
        OutputStream sink = streamSupplier.get();
        try {
            return new BufferedOutputStream(format.compress(sink));
        } catch (IOException ioe) {
            IOUtils.closeAllSilently(sink);
            throw ioe;
        }
    }

    public static InputStream decompress(ThrowingSupplier<InputStream, IOException> streamSupplier) throws IOException {
        if (selectCompressionFormat().equals(ZSTD)) {
            // ZSTD is available, try it.
            try {
                return decompress(streamSupplier, ZSTD);
            } catch (IOException zstdIOe) {
                // It failed, lets try GZIP
                try {
                    return decompress(streamSupplier, GZIP);
                } catch (IOException gzipIOe) {
                    throw Exceptions.chain(zstdIOe, gzipIOe);
                }
            }
        }
        return decompress(streamSupplier, GZIP);
    }

    private static InputStream decompress(
            ThrowingSupplier<InputStream, IOException> streamSupplier, CompressionFormat format) throws IOException {
        InputStream source = streamSupplier.get();
        try {
            return format.decompress(source);
        } catch (IOException ioe) {
            IOUtils.closeAllSilently(source);
            throw ioe;
        }
    }

    public static StandardCompressionFormat selectCompressionFormat() {
        return selectCompressionFormat(null);
    }

    public static StandardCompressionFormat selectCompressionFormat(PrintStream output) {
        try {
            Native.load(); // Try to load ZSTD
            if (Native.isLoaded()) {
                return ZSTD;
            }
        } catch (Throwable t) {
            if (output != null) {
                output.println("Failed to load " + ZSTD.name() + ": " + t.getMessage());
                output.println("Fallback to " + GZIP.name());
            }
        }
        return GZIP; // fallback
    }
}
