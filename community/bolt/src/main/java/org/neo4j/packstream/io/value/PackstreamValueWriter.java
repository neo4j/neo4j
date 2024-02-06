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
package org.neo4j.packstream.io.value;

import io.netty.buffer.Unpooled;
import java.util.List;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public abstract class PackstreamValueWriter implements AnyValueWriter<RuntimeException> {
    protected final PackstreamBuf buf;

    public PackstreamValueWriter(PackstreamBuf target) {
        this.buf = target;
    }

    @Override
    public void writeNull() {
        this.buf.writeNull();
    }

    @Override
    public void writeBoolean(boolean value) {
        this.buf.writeBoolean(value);
    }

    @Override
    public void writeInteger(byte value) {
        this.buf.writeInt(value);
    }

    @Override
    public void writeInteger(short value) {
        this.buf.writeInt(value);
    }

    @Override
    public void writeInteger(int value) {
        this.buf.writeInt(value);
    }

    @Override
    public void writeInteger(long value) {
        this.buf.writeInt(value);
    }

    @Override
    public void writeFloatingPoint(float value) {
        this.buf.writeFloat(value);
    }

    @Override
    public void writeFloatingPoint(double value) {
        this.buf.writeFloat(value);
    }

    @Override
    public void writeString(String value) {
        this.buf.writeString(value);
    }

    @Override
    public void writeString(char value) {
        this.buf.writeString(Character.toString(value));
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        this.buf.writeListHeader(size);
    }

    @Override
    public void endArray() {
        // NOOP - Lists do not require termination
    }

    @Override
    public void writeByteArray(byte[] value) {
        this.buf.writeBytes(Unpooled.wrappedBuffer(value));
    }

    @Override
    public EntityMode entityMode() {
        return EntityMode.FULL;
    }

    @Override
    public void beginMap(int size) {
        this.buf.writeMapHeader(size);
    }

    @Override
    public void endMap() {
        // NOOP - Packstream does not feature explicit termination
    }

    @Override
    public void beginList(int size) {
        this.buf.writeListHeader(size);
    }

    @Override
    public void endList() {
        // NOOP - Packstream does not feature explicit termination
    }

    @Override
    public void writeNodeReference(long nodeId) {
        throw new UnsupportedOperationException("Cannot write raw node reference");
    }

    @Override
    public void writeRelationshipReference(long relId) {
        throw new UnsupportedOperationException("Cannot write raw relationship reference");
    }

    @Override
    public void writePathReference(long[] nodes, long[] relationships) {
        throw new UnsupportedOperationException("Cannot write raw path reference");
    }

    @Override
    public void writePathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships)
            throws RuntimeException {
        throw new UnsupportedOperationException("Cannot write raw path reference");
    }

    @Override
    public void writePathReference(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships)
            throws RuntimeException {
        throw new UnsupportedOperationException("Cannot write raw path reference");
    }
}
