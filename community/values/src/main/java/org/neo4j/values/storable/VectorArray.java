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
package org.neo4j.values.storable;

import java.util.Arrays;
import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.graphdb.Vector;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VectorCandidate;

public class VectorArray extends NonPrimitiveArray<VectorValue> {
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(VectorArray.class);
    private final VectorValue[] vectors;

    VectorArray(VectorValue... vectors) {
        this.vectors = vectors;
    }

    @Override
    public String getTypeName() {
        return "VectorArray";
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.VECTOR_ARRAY;
    }

    @Override
    public VectorValue value(int offset) {
        return Values.vectorValue((Vector) vectors[offset]);
    }

    @Override
    protected VectorValue[] value() {
        return vectors;
    }

    @Override
    public boolean equals(Value other) {
        if (other instanceof VectorArray that) {
            return Arrays.equals(this.vectors, that.vectors);
        }
        return false;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        // todo: needs ValueWriter implementation
        throw new NotImplementedException("needs ValueWriter implementation");
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        // todo: needs ValueMapper implementation
        throw new NotImplementedException("needs ValueMapper implementation");
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof Vector
                || value instanceof VectorCandidate
                || (value instanceof SequenceValue sequence
                        && sequence.asListValue().itemValueRepresentation().valueGroup() == ValueGroup.NUMBER);
    }

    @Override
    public VectorArray copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        VectorValue[] newVectors = new VectorValue[vectors.length + 1];
        System.arraycopy(vectors, 0, newVectors, 0, vectors.length);
        newVectors[vectors.length] = Values.vectorValue(added);
        return new VectorArray(newVectors);
    }

    @Override
    public VectorArray copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        VectorValue[] newVectors = new VectorValue[vectors.length + 1];
        newVectors[0] = Values.vectorValue(prepended);
        System.arraycopy(vectors, 0, newVectors, 1, vectors.length);
        return new VectorArray(newVectors);
    }

    @Override
    public long estimatedHeapUsage() {
        long size = SHALLOW_SIZE;
        for (VectorValue vector : vectors) {
            size += vector.estimatedHeapUsage();
        }
        return size;
    }
}
