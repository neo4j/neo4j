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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;

public class PhysicalLogVersionedStoreChannel extends DelegatingStoreChannel<StoreChannel>
        implements LogVersionedStoreChannel {
    private final long version;
    private final LogFormat formatVersion;
    private long position;
    private final Path path;
    private final ChannelNativeAccessor nativeChannelAccessor;
    private final boolean raw;
    private final DatabaseTracer databaseTracer;

    public PhysicalLogVersionedStoreChannel(
            StoreChannel delegateChannel,
            LogHeader header,
            Path path,
            ChannelNativeAccessor nativeChannelAccessor,
            DatabaseTracer databaseTracer)
            throws IOException {
        this(
                delegateChannel,
                header.getLogVersion(),
                header.getLogFormatVersion(),
                path,
                nativeChannelAccessor,
                databaseTracer,
                false);
    }

    public PhysicalLogVersionedStoreChannel(
            StoreChannel delegateChannel,
            long version,
            LogFormat formatVersion,
            Path path,
            ChannelNativeAccessor nativeChannelAccessor,
            DatabaseTracer databaseTracer)
            throws IOException {
        this(delegateChannel, version, formatVersion, path, nativeChannelAccessor, databaseTracer, false);
    }

    public PhysicalLogVersionedStoreChannel(
            StoreChannel delegateChannel,
            long version,
            LogFormat formatVersion,
            Path path,
            ChannelNativeAccessor nativeChannelAccessor,
            DatabaseTracer databaseTracer,
            boolean raw)
            throws IOException {
        super(delegateChannel);
        this.version = version;
        this.formatVersion = formatVersion;
        this.position = delegateChannel.position();
        this.path = path;
        this.nativeChannelAccessor = nativeChannelAccessor;
        this.databaseTracer = databaseTracer;
        this.raw = raw;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void writeAll(ByteBuffer src, long position) {
        throw new UnsupportedOperationException("Not needed");
    }

    @Override
    public void writeAll(ByteBuffer src) throws IOException {
        advance(src.remaining());
        super.writeAll(src);
    }

    @Override
    public int read(ByteBuffer dst, long position) {
        throw new UnsupportedOperationException("Not needed");
    }

    @Override
    public StoreChannel position(long newPosition) throws IOException {
        this.position = newPosition;
        return super.position(newPosition);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return (int) advance(super.read(dst));
    }

    private long advance(long bytes) {
        if (bytes != -1) {
            position += bytes;
        }
        return bytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return (int) advance(super.write(src));
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void close() throws IOException {
        if (!raw) {
            nativeChannelAccessor.evictFromSystemCache(this, version);
        }
        databaseTracer.closeLogFile(path);
        super.close();
    }

    @Override
    public long write(ByteBuffer[] sources, int offset, int length) throws IOException {
        return advance(super.write(sources, offset, length));
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return advance(super.write(srcs));
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return advance(super.read(dsts, offset, length));
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return advance(super.read(dsts));
    }

    @Override
    public long getLogVersion() {
        return version;
    }

    @Override
    public LogFormat getLogFormatVersion() {
        return formatVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PhysicalLogVersionedStoreChannel that = (PhysicalLogVersionedStoreChannel) o;

        return version == that.version && delegate.equals(that.delegate);
    }

    @Override
    public void flush() throws IOException {
        try (var ignored = databaseTracer.flushFile()) {
            super.flush();
        }
    }

    @Override
    public int hashCode() {
        int result = delegate.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
