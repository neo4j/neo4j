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
package org.neo4j.bolt.protocol.io.writer;

import org.neo4j.bolt.protocol.io.pipeline.WriterContext;

public final class UUIDUnknownTypeVersionedValueWriter extends AbstractUnknownTypeVersionedValueWriter {

    private static final UUIDUnknownTypeVersionedValueWriter INSTANCE = new UUIDUnknownTypeVersionedValueWriter();

    private UUIDUnknownTypeVersionedValueWriter() {}

    public static UUIDUnknownTypeVersionedValueWriter getInstance() {
        return INSTANCE;
    }

    @Override
    protected String typeName() {
        return "UUID";
    }

    @Override
    public void writeUUID(WriterContext ctx, long msb, long lsb) {
        this.reportUnknownType(ctx);
    }
}
