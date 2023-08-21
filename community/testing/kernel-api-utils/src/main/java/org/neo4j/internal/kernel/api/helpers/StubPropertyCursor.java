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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Map;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class StubPropertyCursor extends DefaultCloseListenable implements PropertyCursor {
    private int offset = -1;
    private Integer[] keys;
    private Value[] values;
    private PropertySelection selection;

    public void init(Map<Integer, Value> properties, PropertySelection selection) {
        this.selection = selection;
        offset = -1;
        keys = properties.keySet().toArray(new Integer[0]);
        values = properties.values().toArray(new Value[0]);
    }

    @Override
    public boolean next() {
        while (offset + 1 < keys.length) {
            if (++offset < keys.length && selection.test(keys[offset])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void closeInternal() {}

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public int propertyKey() {
        return keys[offset];
    }

    @Override
    public ValueGroup propertyType() {
        return values[offset].valueGroup();
    }

    @Override
    public Value propertyValue() {
        return values[offset];
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void removeTracer() {
        throw new UnsupportedOperationException("not implemented");
    }
}
