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
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Objects;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Equality;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ValueRepresentation;

public class GraphReferenceValue extends AnyValue {
    private final DatabaseReference dbRef;

    // TODO: should DatabaseReference implement Measurable?
    private static final long SHALLOW_SIZE =
            shallowSizeOfInstance(GraphReferenceValue.class) + shallowSizeOfInstance(DatabaseReference.class);

    public GraphReferenceValue(DatabaseReference dbRef) {
        this.dbRef = dbRef;
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE;
    }

    public DatabaseReference getDbRef() {
        return dbRef;
    }

    @Override
    protected boolean equalTo(Object other) {
        if (other instanceof GraphReferenceValue val) {
            return this.dbRef == val.dbRef;
        }
        return false;
    }

    @Override
    protected int computeHash() {
        return Objects.hashCode(this);
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        throw new UnsupportedOperationException("GraphReferenceValue.writeTo not implemented");
    }

    @Override
    public Equality ternaryEquals(AnyValue other) {
        if (other == NO_VALUE) {
            return Equality.UNDEFINED;
        }
        if (equalTo(other)) {
            return Equality.TRUE;
        }
        return Equality.FALSE;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        throw new UnsupportedOperationException("GraphReferenceValue.map not implemented");
    }

    @Override
    public String getTypeName() {
        return "GraphReference";
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.UNKNOWN;
    }
}
