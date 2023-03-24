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
package org.neo4j.internal.schema;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;

public enum SchemaScalarValueType implements SchemaValueType {
    BOOLEAN(ValueRepresentation.BOOLEAN),
    BOOL(BOOLEAN),
    INTEGER(ValueRepresentation.INT64, ValueRepresentation.INT32, ValueRepresentation.INT16, ValueRepresentation.INT8),
    INT(INTEGER),
    INTEGER64(INTEGER);

    private final List<ValueRepresentation> valueRepresentations;
    private final SchemaScalarValueType delegate;

    SchemaScalarValueType(SchemaScalarValueType delegate) {
        this.delegate = delegate;
        this.valueRepresentations = null;
    }

    SchemaScalarValueType(ValueRepresentation valueRepresentation, ValueRepresentation... valueRepresentations) {
        this.delegate = null;
        this.valueRepresentations = new ArrayList<>(valueRepresentations.length + 1);
        this.valueRepresentations.add(valueRepresentation);
        for (int i = 0; i < valueRepresentations.length; i++) {
            this.valueRepresentations.add(valueRepresentations[i]);
        }
    }

    @Override
    public boolean isAssignable(Value value) {
        if (delegate != null) {
            return delegate.isAssignable(value);
        }

        return isAssignable(value.valueRepresentation());
    }

    boolean isAssignable(ValueRepresentation valueRepresentation) {
        for (ValueRepresentation vr : valueRepresentations) {
            if (vr == valueRepresentation) {
                return true;
            }
        }

        return false;
    }
}
