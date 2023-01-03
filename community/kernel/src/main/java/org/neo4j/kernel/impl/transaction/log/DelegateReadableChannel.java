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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;

public class DelegateReadableChannel implements ReadableClosablePositionAwareChecksumChannel {
    private ReadableChannel delegate;

    public void delegateTo(ReadableChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte get() throws IOException {
        assertAssigned();
        return delegate.get();
    }

    @Override
    public short getShort() throws IOException {
        assertAssigned();
        return delegate.getShort();
    }

    @Override
    public int getInt() throws IOException {
        assertAssigned();
        return delegate.getInt();
    }

    @Override
    public long getLong() throws IOException {
        assertAssigned();
        return delegate.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        assertAssigned();
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        assertAssigned();
        return delegate.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        assertAssigned();
        delegate.get(bytes, length);
    }

    @Override
    public LogPositionMarker getCurrentPosition(LogPositionMarker positionMarker) {
        assertAssigned();
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentPosition() {
        assertAssigned();
        return LogPosition.UNSPECIFIED;
    }

    private void assertAssigned() {
        if (delegate == null) {
            throw new IllegalArgumentException("No assigned channel to delegate reads");
        }
    }

    @Override
    public void close() {
        // no op
    }

    @Override
    public void beginChecksum() {
        // no op
    }

    @Override
    public int getChecksum() {
        // no op
        return 0;
    }

    @Override
    public int endChecksumAndValidate() {
        // no op
        return 0;
    }
}
