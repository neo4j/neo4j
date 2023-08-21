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
package org.neo4j.internal.kernel.api.procs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class DefaultParameterValue {
    private final Object value;
    private final Neo4jTypes.AnyType type;

    public DefaultParameterValue(Object value, Neo4jTypes.AnyType type) {
        this.value = value;
        this.type = type;
    }

    public Object value() {
        return value;
    }

    public Object javaValue() {
        return unifyType(value);
    }

    public Neo4jTypes.AnyType neo4jType() {
        return type;
    }

    public static DefaultParameterValue ntString(String value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTString);
    }

    public static DefaultParameterValue ntInteger(long value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTInteger);
    }

    public static DefaultParameterValue ntFloat(double value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTFloat);
    }

    public static DefaultParameterValue ntBoolean(boolean value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTBoolean);
    }

    public static DefaultParameterValue ntMap(Map<String, Object> value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTMap);
    }

    public static DefaultParameterValue ntByteArray(byte[] value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTByteArray);
    }

    public static DefaultParameterValue ntList(List<?> value, Neo4jTypes.AnyType inner) {
        return new DefaultParameterValue(value, Neo4jTypes.NTList(inner));
    }

    public static DefaultParameterValue ntAny(Object value) {
        return new DefaultParameterValue(value, Neo4jTypes.NTAny);
    }

    public static DefaultParameterValue nullValue(Neo4jTypes.AnyType type) {
        return new DefaultParameterValue(null, type);
    }

    public DefaultParameterValue castAs(Neo4jTypes.AnyType type) {
        return new DefaultParameterValue(value, type);
    }

    @Override
    public String toString() {
        return "DefaultParameterValue{" + "value=" + javaValue() + ", type=" + type + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultParameterValue that = (DefaultParameterValue) o;

        if (!type.equals(that.type)) {
            return false;
        }
        if (type.equals(Neo4jTypes.NTByteArray)) {
            return Arrays.deepEquals(new Object[] {value}, new Object[] {that.value});
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        return 31 * result + type.hashCode();
    }

    /**
     * Make sure we always return plain java objects here so we get a consistent signature
     */
    private Object unifyType(Object obj) {
        if (obj instanceof Value) {
            return ((Value) obj).map(new ValueMapper.JavaMapper() {
                @Override
                public Object mapPath(VirtualPathValue value) {
                    throw new UnsupportedOperationException("Not allowed as default values ");
                }

                @Override
                public Object mapNode(VirtualNodeValue value) {
                    throw new UnsupportedOperationException("Not allowed as default values ");
                }

                @Override
                public Object mapRelationship(VirtualRelationshipValue value) {
                    throw new UnsupportedOperationException("Not allowed as default values ");
                }
            });
        } else {
            return obj;
        }
    }
}
