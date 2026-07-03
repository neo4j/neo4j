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
package org.neo4j.server.queryapi.driver.boltmessage.codec;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.driver.internal.InternalFloat32Vector;
import org.neo4j.driver.internal.InternalFloat64Vector;
import org.neo4j.driver.internal.InternalInt16Vector;
import org.neo4j.driver.internal.InternalInt32Vector;
import org.neo4j.driver.internal.InternalInt64Vector;
import org.neo4j.driver.internal.InternalInt8Vector;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.BytesValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Path;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class BoltMessageValueDecoder {

    private static final JavaDriverInternalValueMapper MAPPER = new JavaDriverInternalValueMapper();

    public static Value decode(AnyValue value) {
        return value.map(MAPPER);
    }

    public static class JavaDriverInternalValueMapper implements ValueMapper<InternalValue> {
        @Override
        public InternalValue mapPath(VirtualPathValue value) {
            if (value instanceof PathValue pathValue) {
                return new org.neo4j.driver.internal.value.PathValue(convertPath(pathValue));
            } else {
                throw new UnsupportedOperationException("Unsupported path type: " + value.getClass());
            }
        }

        private Path convertPath(PathValue pathValue) {
            var entityList = new ArrayList<Entity>();
            pathValue.asList().forEach(kernelEntity -> {
                switch (kernelEntity) {
                    case NodeValue nodeValue -> {
                        entityList.add(new InternalNode(
                                nodeValue.id(),
                                nodeValue.elementId(),
                                convertLabels(nodeValue.labels()),
                                convertProperties(nodeValue.properties(), this)));
                    }
                    case RelationshipValue relationshipValue -> {
                        entityList.add(new InternalRelationship(
                                relationshipValue.id(),
                                relationshipValue.elementId(),
                                relationshipValue.startNodeId(),
                                relationshipValue.startNodeElementId(),
                                relationshipValue.endNodeId(),
                                relationshipValue.endNodeElementId(),
                                relationshipValue.type().stringValue(),
                                convertProperties(relationshipValue.properties(), this)));
                    }
                    default -> {
                        throw new UnsupportedOperationException(
                                "Unsupported path entity type: " + kernelEntity.getClass());
                    }
                }
            });
            return new InternalPath(entityList);
        }

        @Override
        public InternalValue mapNode(VirtualNodeValue value) {
            if (value instanceof NodeValue nodeValue) {
                return new org.neo4j.driver.internal.value.NodeValue(new InternalNode(
                        nodeValue.id(),
                        nodeValue.elementId(),
                        convertLabels(nodeValue.labels()),
                        convertProperties(nodeValue.properties(), this)));
            } else {
                throw new UnsupportedOperationException("Unsupported node type: " + value.getClass());
            }
        }

        @Override
        public InternalValue mapRelationship(VirtualRelationshipValue value) {
            if (value instanceof RelationshipValue relationshipValue) {
                return new org.neo4j.driver.internal.value.RelationshipValue(new InternalRelationship(
                        relationshipValue.id(),
                        relationshipValue.elementId(),
                        relationshipValue.startNodeId(),
                        relationshipValue.startNodeElementId(),
                        relationshipValue.endNodeId(),
                        relationshipValue.endNodeElementId(),
                        relationshipValue.type().stringValue(),
                        convertProperties(relationshipValue.properties(), this)));
            } else {
                throw new UnsupportedOperationException("Unsupported relationship type: " + value.getClass());
            }
        }

        @Override
        public InternalValue mapMap(MapValue value) {
            var map = new HashMap<String, org.neo4j.driver.Value>();
            value.foreach((k, v) -> map.put(k, v.map(this)));
            return new org.neo4j.driver.internal.value.MapValue(map);
        }

        @Override
        public InternalValue mapNoValue() {
            return (InternalValue) NullValue.NULL;
        }

        @Override
        public InternalValue mapSequence(SequenceValue value) {
            var anotherList = new ArrayList<org.neo4j.driver.Value>();
            value.forEach(v -> anotherList.add(v.map(this)));
            return new ListValue(anotherList);
        }

        @Override
        public InternalValue mapByteArray(ByteArray value) {
            return new BytesValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapText(TextValue value) {
            return new StringValue(value.stringValue());
        }

        @Override
        public InternalValue mapBoolean(BooleanValue value) {
            return value.booleanValue()
                    ? org.neo4j.driver.internal.value.BooleanValue.TRUE
                    : org.neo4j.driver.internal.value.BooleanValue.FALSE;
        }

        @Override
        public InternalValue mapNumber(NumberValue value) {
            return switch (value) {
                case IntegralValue integralValue -> new IntegerValue(integralValue.longValue());
                case FloatingPointValue floatingPointValue ->
                    new org.neo4j.driver.internal.value.FloatValue(floatingPointValue.doubleValue());
                default -> throw new UnsupportedOperationException("Unsupported number type: " + value.getClass());
            };
        }

        @Override
        public InternalValue mapDateTime(DateTimeValue value) {
            return new org.neo4j.driver.internal.value.DateTimeValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapLocalDateTime(LocalDateTimeValue value) {
            return new org.neo4j.driver.internal.value.LocalDateTimeValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapDate(DateValue value) {
            return new org.neo4j.driver.internal.value.DateValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapTime(TimeValue value) {
            return new org.neo4j.driver.internal.value.TimeValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapLocalTime(LocalTimeValue value) {
            return new org.neo4j.driver.internal.value.LocalTimeValue(value.asObjectCopy());
        }

        @Override
        public InternalValue mapDuration(DurationValue value) {
            var temporalCopy = value.asObjectCopy();
            var isoDuration = new InternalIsoDuration(
                    temporalCopy.get(ChronoUnit.MONTHS),
                    temporalCopy.get(ChronoUnit.DAYS),
                    temporalCopy.get(ChronoUnit.SECONDS),
                    // Should be safe to cast here since a nanosecond fits within the int type
                    // with second increments fitting into the above count
                    (int) temporalCopy.get(ChronoUnit.NANOS));
            return new org.neo4j.driver.internal.value.DurationValue(isoDuration);
        }

        @Override
        public InternalValue mapPoint(PointValue value) {
            var pointCopy = value.asObjectCopy();
            var coordinate = pointCopy.getCoordinates().get(0).getCoordinate();
            if (coordinate.length == 2) {
                var driverPointValue = new InternalPoint2D(pointCopy.getCRS().getCode(), coordinate[0], coordinate[1]);
                return new org.neo4j.driver.internal.value.PointValue(driverPointValue);
            } else {
                var driverPointValue =
                        new InternalPoint3D(pointCopy.getCRS().getCode(), coordinate[0], coordinate[1], coordinate[2]);
                return new org.neo4j.driver.internal.value.PointValue(driverPointValue);
            }
        }

        @Override
        public InternalValue mapVector(VectorValue value) {
            return switch (value) {
                case Int8Vector integer8 ->
                    new org.neo4j.driver.internal.value.VectorValue(new InternalInt8Vector(toByteArray(integer8)));
                case Int16Vector integer16 ->
                    new org.neo4j.driver.internal.value.VectorValue(new InternalInt16Vector(toShortArray(integer16)));
                case Int32Vector integer32 ->
                    new org.neo4j.driver.internal.value.VectorValue(new InternalInt32Vector(toIntArray(integer32)));
                case Int64Vector integer64 ->
                    new org.neo4j.driver.internal.value.VectorValue(new InternalInt64Vector(toLongArray(integer64)));
                case Float32Vector floatVector ->
                    new org.neo4j.driver.internal.value.VectorValue(
                            new InternalFloat32Vector(toFloatArray(floatVector)));
                case Float64Vector floatVector ->
                    new org.neo4j.driver.internal.value.VectorValue(
                            new InternalFloat64Vector(toDoubleArray(floatVector)));
            };
        }

        @Override
        public InternalValue mapUUID(UUIDValue value) {
            return new org.neo4j.driver.internal.value.UUIDValue(value.asObjectCopy());
        }
    }

    private static byte[] toByteArray(Int8Vector v) {
        byte[] arr = new byte[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = (byte) v.doubleValue(i);
        return arr;
    }

    private static short[] toShortArray(Int16Vector v) {
        short[] arr = new short[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = (short) v.doubleValue(i);
        return arr;
    }

    private static int[] toIntArray(Int32Vector v) {
        int[] arr = new int[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = (int) v.doubleValue(i);
        return arr;
    }

    private static long[] toLongArray(Int64Vector v) {
        long[] arr = new long[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = (long) v.doubleValue(i);
        return arr;
    }

    private static float[] toFloatArray(Float32Vector v) {
        float[] arr = new float[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = v.floatValue(i);
        return arr;
    }

    private static double[] toDoubleArray(Float64Vector v) {
        double[] arr = new double[v.dimensions()];
        for (int i = 0; i < arr.length; i++) arr[i] = v.doubleValue(i);
        return arr;
    }

    private static Map<String, org.neo4j.driver.Value> convertProperties(
            MapValue properties, ValueMapper<InternalValue> mapper) {
        var convertedProperties = new HashMap<String, org.neo4j.driver.Value>();
        properties.foreach((k, v) -> convertedProperties.put(k, v.map(mapper)));
        return convertedProperties;
    }

    private static Collection<String> convertLabels(TextArray labels) {
        var hashSet = new ArrayList<String>();
        for (var label : labels) {
            switch (label) {
                case TextValue textValue -> hashSet.add(textValue.stringValue());
                default -> throw new UnsupportedOperationException("Unsupported number type: " + label.getTypeName());
            }
        }
        return hashSet;
    }
}
