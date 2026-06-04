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
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;

@Deprecated(since = "2025.7", forRemoval = true)
public final class VectorUnknownTypeVersionedValueWriter extends AbstractUnknownTypeVersionedValueWriter {
    private static final VectorUnknownTypeVersionedValueWriter INSTANCE = new VectorUnknownTypeVersionedValueWriter();

    private VectorUnknownTypeVersionedValueWriter() {}

    public static VectorUnknownTypeVersionedValueWriter getInstance() {
        return INSTANCE;
    }

    @Override
    protected String typeName() {
        return "VECTOR";
    }

    private String generateTypeDescription(int dimension, String type) {
        return String.format("VECTOR(%d, %s)", dimension, type);
    }

    @Override
    public void writeVector(WriterContext ctx, byte[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Int8Vector.NESTED_TYPE_NAME));
    }

    @Override
    public void writeVector(WriterContext ctx, short[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Int16Vector.NESTED_TYPE_NAME));
    }

    @Override
    public void writeVector(WriterContext ctx, int[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Int32Vector.NESTED_TYPE_NAME));
    }

    @Override
    public void writeVector(WriterContext ctx, long[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Int64Vector.NESTED_TYPE_NAME));
    }

    @Override
    public void writeVector(WriterContext ctx, float[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Float32Vector.NESTED_TYPE_NAME));
    }

    @Override
    public void writeVector(WriterContext ctx, double[] values) {
        this.reportUnknownType(ctx, this.generateTypeDescription(values.length, Float64Vector.NESTED_TYPE_NAME));
    }
}
