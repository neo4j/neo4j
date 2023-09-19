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
import org.neo4j.cypher.internal.ast.BooleanTypeName;
import org.neo4j.cypher.internal.ast.ClosedDynamicUnionTypeName;
import org.neo4j.cypher.internal.ast.CypherTypeName;
import org.neo4j.cypher.internal.ast.DateTypeName;
import org.neo4j.cypher.internal.ast.DurationTypeName;
import org.neo4j.cypher.internal.ast.FloatTypeName;
import org.neo4j.cypher.internal.ast.IntegerTypeName;
import org.neo4j.cypher.internal.ast.ListTypeName;
import org.neo4j.cypher.internal.ast.LocalDateTimeTypeName;
import org.neo4j.cypher.internal.ast.LocalTimeTypeName;
import org.neo4j.cypher.internal.ast.MapTypeName;
import org.neo4j.cypher.internal.ast.NodeTypeName;
import org.neo4j.cypher.internal.ast.NothingTypeName;
import org.neo4j.cypher.internal.ast.NullTypeName;
import org.neo4j.cypher.internal.ast.PathTypeName;
import org.neo4j.cypher.internal.ast.PointTypeName;
import org.neo4j.cypher.internal.ast.RelationshipTypeName;
import org.neo4j.cypher.internal.ast.StringTypeName;
import org.neo4j.cypher.internal.ast.ZonedDateTimeTypeName;
import org.neo4j.cypher.internal.ast.ZonedTimeTypeName;
import org.neo4j.cypher.internal.util.InputPosition;
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

public final class CypherTypeNameValueMapper implements ValueMapper<CypherTypeName> {

    private static final InputPosition dummyPos = InputPosition.NONE();
    private static final NullTypeName NULL_CYPHER_TYPE_NAME = new NullTypeName(dummyPos);
    private static final BooleanTypeName BOOLEAN_CYPHER_TYPE_NAME = new BooleanTypeName(false, dummyPos);
    private static final StringTypeName STRING_CYPHER_TYPE_NAME = new StringTypeName(false, dummyPos);
    private static final IntegerTypeName INTEGER_CYPHER_TYPE_NAME = new IntegerTypeName(false, dummyPos);
    private static final FloatTypeName FLOAT_CYPHER_TYPE_NAME = new FloatTypeName(false, dummyPos);
    private static final DateTypeName DATE_CYPHER_TYPE_NAME = new DateTypeName(false, dummyPos);
    private static final LocalTimeTypeName LOCAL_TIME_CYPHER_TYPE_NAME = new LocalTimeTypeName(false, dummyPos);
    private static final ZonedTimeTypeName ZONED_TIME_CYPHER_TYPE_NAME = new ZonedTimeTypeName(false, dummyPos);
    private static final LocalDateTimeTypeName LOCAL_DATETIME_CYPHER_TYPE_NAME =
            new LocalDateTimeTypeName(false, dummyPos);
    private static final ZonedDateTimeTypeName ZONED_DATETIME_CYPHER_TYPE_NAME =
            new ZonedDateTimeTypeName(false, dummyPos);
    private static final DurationTypeName DURATION_CYPHER_TYPE_NAME = new DurationTypeName(false, dummyPos);
    private static final PointTypeName POINT_CYPHER_TYPE_NAME = new PointTypeName(false, dummyPos);
    private static final NodeTypeName NODE_CYPHER_TYPE_NAME = new NodeTypeName(false, dummyPos);
    private static final RelationshipTypeName RELATIONSHIP_CYPHER_TYPE_NAME = new RelationshipTypeName(false, dummyPos);
    private static final MapTypeName MAP_CYPHER_TYPE_NAME = new MapTypeName(false, dummyPos);
    private static final PathTypeName PATH_CYPHER_TYPE_NAME = new PathTypeName(false, dummyPos);
    private static final ListTypeName LIST_BOOLEAN_CYPHER_TYPE_NAME =
            new ListTypeName(BOOLEAN_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_STRING_CYPHER_TYPE_NAME =
            new ListTypeName(STRING_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_INTEGER_CYPHER_TYPE_NAME =
            new ListTypeName(INTEGER_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_FLOAT_CYPHER_TYPE_NAME =
            new ListTypeName(FLOAT_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_DATE_CYPHER_TYPE_NAME =
            new ListTypeName(DATE_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_LOCAL_TIME_CYPHER_TYPE_NAME =
            new ListTypeName(LOCAL_TIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_ZONED_TIME_CYPHER_TYPE_NAME =
            new ListTypeName(ZONED_TIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_LOCAL_DATETIME_CYPHER_TYPE_NAME =
            new ListTypeName(LOCAL_DATETIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_ZONED_DATETIME_CYPHER_TYPE_NAME =
            new ListTypeName(ZONED_DATETIME_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_DURATION_CYPHER_TYPE_NAME =
            new ListTypeName(DURATION_CYPHER_TYPE_NAME, false, dummyPos);
    private static final ListTypeName LIST_POINT_CYPHER_TYPE_NAME =
            new ListTypeName(POINT_CYPHER_TYPE_NAME, false, dummyPos);

    @Override
    public CypherTypeName mapNoValue() {
        return NULL_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapBoolean(BooleanValue value) {
        return BOOLEAN_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapText(TextValue value) {
        return STRING_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapNumber(NumberValue value) {
        if (value instanceof IntegralValue) {
            return INTEGER_CYPHER_TYPE_NAME;
        }
        return FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapIntegral(IntegralValue value) {
        return INTEGER_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapFloatingPoint(FloatingPointValue value) {
        return FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDate(DateValue value) {
        return DATE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapLocalTime(LocalTimeValue value) {
        return LOCAL_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapTime(TimeValue value) {
        return ZONED_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapLocalDateTime(LocalDateTimeValue value) {
        return LOCAL_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDateTime(DateTimeValue value) {
        return ZONED_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDuration(DurationValue value) {
        return DURATION_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapPoint(PointValue value) {
        return POINT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapNode(VirtualNodeValue value) {
        return NODE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapRelationship(VirtualRelationshipValue value) {
        return RELATIONSHIP_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapMap(MapValue value) {
        return MAP_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapSequence(SequenceValue value) {
        if (value.isEmpty()) {
            return new ListTypeName(new NothingTypeName(dummyPos), false, dummyPos);
        } else if (value instanceof ArrayValue array) {
            // ArrayValues are arrays of one type, so the first value is enough to check
            return new ListTypeName(array.value(0).map(this), false, dummyPos);
        }
        Set<CypherTypeName> innerTypes = new HashSet<>();
        value.forEach(listItem -> innerTypes.add(listItem.map(this)));
        return new ListTypeName(
                new ClosedDynamicUnionTypeName(asScala(innerTypes).toSet(), dummyPos), false, dummyPos);
    }

    @Override
    public CypherTypeName mapTextArray(TextArray value) {
        if (value.isEmpty()) {
            return new ListTypeName(new NothingTypeName(dummyPos), false, dummyPos);
        } else {
            return LIST_STRING_CYPHER_TYPE_NAME;
        }
    }

    @Override
    public CypherTypeName mapBooleanArray(BooleanArray value) {
        return LIST_BOOLEAN_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapNumberArray(NumberArray value) {
        return mapSequence(value);
    }

    @Override
    public CypherTypeName mapIntegralArray(IntegralArray value) {
        return LIST_INTEGER_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapFloatingPointArray(FloatingPointArray value) {
        return LIST_FLOAT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDateArray(DateArray value) {
        return LIST_DATE_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapLocalTimeArray(LocalTimeArray value) {
        return LIST_LOCAL_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapTimeArray(TimeArray value) {
        return LIST_ZONED_TIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapLocalDateTimeArray(LocalDateTimeArray value) {
        return LIST_LOCAL_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDateTimeArray(DateTimeArray value) {
        return LIST_ZONED_DATETIME_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapPointArray(PointArray value) {
        return LIST_POINT_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapDurationArray(DurationArray value) {
        return LIST_DURATION_CYPHER_TYPE_NAME;
    }

    @Override
    public CypherTypeName mapPath(VirtualPathValue value) {
        return PATH_CYPHER_TYPE_NAME;
    }
}
