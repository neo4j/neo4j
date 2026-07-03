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

import org.neo4j.bolt.connection.values.IsoDuration;
import org.neo4j.bolt.connection.values.Point;
import org.neo4j.bolt.connection.values.Vector;
import org.neo4j.driver.types.Float32Vector;
import org.neo4j.driver.types.Float64Vector;
import org.neo4j.driver.types.Int16Vector;
import org.neo4j.driver.types.Int32Vector;
import org.neo4j.driver.types.Int64Vector;
import org.neo4j.driver.types.Int8Vector;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

public class BoltMessageValueEncoder {

    public static AnyValue encode(org.neo4j.bolt.connection.values.Value driverValue) {
        if (driverValue == null) {
            return Values.NO_VALUE;
        }

        switch (driverValue.boltValueType()) {
            case BOOLEAN -> {
                return encodeBoolean(driverValue.asBoolean());
            }
            case BYTES -> {
                return Values.byteArray(driverValue.asByteArray());
            }
            case STRING -> {
                return Values.stringValue(driverValue.asString());
            }
            case INTEGER -> {
                return Values.longValue(driverValue.asLong());
            }
            case FLOAT -> {
                return Values.doubleValue(driverValue.asDouble());
            }
            case DATE -> {
                return Values.temporalValue(driverValue.asLocalDate());
            }
            case TIME -> {
                return Values.temporalValue(driverValue.asOffsetTime());
            }
            case LOCAL_TIME -> {
                return Values.temporalValue(driverValue.asLocalTime());
            }
            case LOCAL_DATE_TIME -> {
                return Values.temporalValue(driverValue.asLocalDateTime());
            }
            case DATE_TIME -> {
                return Values.temporalValue(driverValue.asZonedDateTime());
            }
            case DURATION -> {
                return encodeDuration(driverValue.asBoltIsoDuration());
            }

            case POINT -> {
                return encodePoint(driverValue.asBoltPoint());
            }
            case VECTOR -> {
                return encodeVector(driverValue.asBoltVector());
            }
            case LIST -> {
                var serverListBuilder = ListValueBuilder.newListBuilder(driverValue.size());
                driverValue.boltValues().forEach(v -> serverListBuilder.add(encode(v)));
                return serverListBuilder.build();
            }
            case MAP -> {
                var convertedMap = new MapValueBuilder(driverValue.size());
                driverValue.asBoltMap().forEach((k, v) -> convertedMap.add(k, encode(v)));
                return convertedMap.build();
            }
            case NULL -> {
                return Values.NO_VALUE;
            }
            case UUID -> {
                return Values.uuidValue(driverValue.asUUID());
            }
            default ->
                throw new UnsupportedOperationException("Unsupported value type: " + driverValue.boltValueType());
        }
    }

    private static BooleanValue encodeBoolean(Boolean driverBoolean) {
        return driverBoolean ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    private static DurationValue encodeDuration(IsoDuration isoDuration) {
        return DurationValue.duration(
                isoDuration.months(), isoDuration.days(), isoDuration.seconds(), isoDuration.nanoseconds());
    }

    private static PointValue encodePoint(Point driverPoint) {
        if (Double.isNaN(driverPoint.z())) {
            return Values.pointValue(
                    CoordinateReferenceSystem.get(driverPoint.srid()), driverPoint.x(), driverPoint.y());
        } else {
            return Values.pointValue(
                    CoordinateReferenceSystem.get(driverPoint.srid()),
                    driverPoint.x(),
                    driverPoint.y(),
                    driverPoint.z());
        }
    }

    private static VectorValue encodeVector(Vector driverVector) {
        switch (driverVector) {
            case Int8Vector int8Vector -> {
                return Values.int8Vector(int8Vector.toArray());
            }
            case Int16Vector int16Vector -> {
                return Values.int16Vector(int16Vector.toArray());
            }
            case Int32Vector int32Vector -> {
                return Values.int32Vector(int32Vector.toArray());
            }
            case Int64Vector int64Vector -> {
                return Values.int64Vector(int64Vector.toArray());
            }
            case Float32Vector float32Vector -> {
                return Values.float32Vector(float32Vector.toArray());
            }
            case Float64Vector float64Vector -> {
                return Values.float64Vector(float64Vector.toArray());
            }
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + driverVector.getClass());
        }
    }
}
