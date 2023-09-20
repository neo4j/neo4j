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
package org.neo4j.cypher.operations;

import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.util.HashSet;
import java.util.Set;
import org.neo4j.cypher.internal.util.InputPosition;
import org.neo4j.cypher.internal.util.symbols.BooleanType;
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType;
import org.neo4j.cypher.internal.util.symbols.CypherType;
import org.neo4j.cypher.internal.util.symbols.DateType;
import org.neo4j.cypher.internal.util.symbols.DurationType;
import org.neo4j.cypher.internal.util.symbols.FloatType;
import org.neo4j.cypher.internal.util.symbols.IntegerType;
import org.neo4j.cypher.internal.util.symbols.ListType;
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType;
import org.neo4j.cypher.internal.util.symbols.LocalTimeType;
import org.neo4j.cypher.internal.util.symbols.MapType;
import org.neo4j.cypher.internal.util.symbols.NodeType;
import org.neo4j.cypher.internal.util.symbols.NothingType;
import org.neo4j.cypher.internal.util.symbols.NullType;
import org.neo4j.cypher.internal.util.symbols.PathType;
import org.neo4j.cypher.internal.util.symbols.PointType;
import org.neo4j.cypher.internal.util.symbols.RelationshipType;
import org.neo4j.cypher.internal.util.symbols.StringType;
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType;
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateArray;
import org.neo4j.values.storable.DateTimeArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationArray;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeArray;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeArray;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public final class CypherTypeValueMapper implements ValueMapper<CypherType> {

    private static final InputPosition dummyPos = InputPosition.NONE();
    private static final NullType NULL_CYPHER_TYPE_NAME = new NullType(dummyPos);
    private static final BooleanType BOOLEAN_CYPHER_TYPE_NAME = new BooleanType(false, dummyPos);
    private static final StringType STRING_CYPHER_TYPE_NAME = new StringType(false, dummyPos);
    private static final IntegerType INTEGER_CYPHER_TYPE_NAME = new IntegerType(false, dummyPos);
    private static final FloatType FLOAT_CYPHER_TYPE_NAME = new FloatType(false, dummyPos);
    private static final DateType DATE_CYPHER_TYPE_NAME = new DateType(false, dummyPos);
    private static final LocalTimeType LOCAL_TIME_CYPHER_TYPE_NAME = new LocalTimeType(false, dummyPos);
    private static final ZonedTimeType ZONED_TIME_CYPHER_TYPE_NAME = new ZonedTimeType(false, dummyPos);
    private static final LocalDateTimeType LOCAL_DATETIME_CYPHER_TYPE_NAME = new LocalDateTimeType(false, dummyPos);
    private static final ZonedDateTimeType ZONED_DATETIME_CYPHER_TYPE_NAME = new ZonedDateTimeType(false, dummyPos);
    private static final DurationType DURATION_CYPHER_TYPE_NAME = new DurationType(false, dummyPos);
    private static final PointType POINT_CYPHER_TYPE_NAME = new PointType(false, dummyPos);
    private static final NodeType NODE_CYPHER_TYPE_NAME = new NodeType(false, dummyPos);
    private static final RelationshipType RELATIONSHIP_CYPHER_TYPE_NAME = new RelationshipType(false, dummyPos);
    private static final MapType MAP_CYPHER_TYPE_NAME = new MapType(false, dummyPos);
    private static final PathType PATH_CYPHER_TYPE_NAME = new PathType(false, dummyPos);
    private static final ListType LIST_BOOLEAN_CYPHER_TYPE_NAME =
            new ListType(BOOLEAN_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_STRING_CYPHER_TYPE_NAME = new ListType(STRING_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_INTEGER_CYPHER_TYPE_NAME =
            new ListType(INTEGER_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_FLOAT_CYPHER_TYPE_NAME = new ListType(FLOAT_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_DATE_CYPHER_TYPE_NAME = new ListType(DATE_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_LOCAL_TIME_CYPHER_TYPE_NAME =
            new ListType(LOCAL_TIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_ZONED_TIME_CYPHER_TYPE_NAME =
            new ListType(ZONED_TIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_LOCAL_DATETIME_CYPHER_TYPE_NAME =
            new ListType(LOCAL_DATETIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_ZONED_DATETIME_CYPHER_TYPE_NAME =
            new ListType(ZONED_DATETIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_DURATION_CYPHER_TYPE_NAME =
            new ListType(DURATION_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListType LIST_POINT_CYPHER_TYPE_NAME = new ListType(POINT_CYPHER_TYPE_NAME, false, dummyPos);

    @Override
    public CypherType mapNoValue() {
        return NULL_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapBoolean(BooleanValue value) {
        return BOOLEAN_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapText(TextValue value) {
        return STRING_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapNumber(NumberValue value) {
        if (value instanceof IntegralValue) {
            return INTEGER_CYPHER_TYPE_NAME;
        }
        return FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapIntegral(IntegralValue value) {
        return INTEGER_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapFloatingPoint(FloatingPointValue value) {
        return FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDate(DateValue value) {
        return DATE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapLocalTime(LocalTimeValue value) {
        return LOCAL_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapTime(TimeValue value) {
        return ZONED_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapLocalDateTime(LocalDateTimeValue value) {
        return LOCAL_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDateTime(DateTimeValue value) {
        return ZONED_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDuration(DurationValue value) {
        return DURATION_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapPoint(PointValue value) {
        return POINT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapNode(VirtualNodeValue value) {
        return NODE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapRelationship(VirtualRelationshipValue value) {
        return RELATIONSHIP_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapMap(MapValue value) {
        return MAP_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapSequence(SequenceValue value) {
        if (value.isEmpty()) {
            return new ListType(new NothingType(dummyPos), false, dummyPos);
        } else if (value instanceof ArrayValue array) {
            // ArrayValues are arrays of one type, so the first value is enough to check
            return new ListType(array.value(0).map(this), false, dummyPos);
        }
        Set<CypherType> innerTypes = new HashSet<>();
        value.forEach(listItem -> innerTypes.add(listItem.map(this)));
        return new ListType(new ClosedDynamicUnionType(asScala(innerTypes).toSet(), dummyPos), false, dummyPos);
    }

    @Override
    public CypherType mapTextArray(TextArray value) {
        if (value.isEmpty()) {
            return new ListType(new NothingType(dummyPos), false, dummyPos);
        } else {
            return LIST_STRING_CYPHER_TYPE_NAME;
        }
    }

    @Override
    public CypherType mapBooleanArray(BooleanArray value) {
        return LIST_BOOLEAN_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapNumberArray(NumberArray value) {
        return mapSequence(value);
    }

    @Override
    public CypherType mapIntegralArray(IntegralArray value) {
        return LIST_INTEGER_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapFloatingPointArray(FloatingPointArray value) {
        return LIST_FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDateArray(DateArray value) {
        return LIST_DATE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapLocalTimeArray(LocalTimeArray value) {
        return LIST_LOCAL_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapTimeArray(TimeArray value) {
        return LIST_ZONED_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapLocalDateTimeArray(LocalDateTimeArray value) {
        return LIST_LOCAL_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDateTimeArray(DateTimeArray value) {
        return LIST_ZONED_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapPointArray(PointArray value) {
        return LIST_POINT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapDurationArray(DurationArray value) {
        return LIST_DURATION_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherType mapPath(VirtualPathValue value) {
        return PATH_CYPHER_TYPE_NAME;
    }
}
