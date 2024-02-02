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

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static org.neo4j.cypher.operations.CursorUtils.propertyKeys;
import static org.neo4j.internal.kernel.api.Read.NO_ID;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.asList;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.neo4j.cypher.internal.expressions.NormalForm;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.cypher.internal.util.symbols.AnyType;
import org.neo4j.cypher.internal.util.symbols.BooleanType;
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType;
import org.neo4j.cypher.internal.util.symbols.CypherType;
import org.neo4j.cypher.internal.util.symbols.DateType;
import org.neo4j.cypher.internal.util.symbols.DurationType;
import org.neo4j.cypher.internal.util.symbols.FloatType;
import org.neo4j.cypher.internal.util.symbols.GeometryType;
import org.neo4j.cypher.internal.util.symbols.IntegerType;
import org.neo4j.cypher.internal.util.symbols.ListType;
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType;
import org.neo4j.cypher.internal.util.symbols.LocalTimeType;
import org.neo4j.cypher.internal.util.symbols.MapType;
import org.neo4j.cypher.internal.util.symbols.NodeType;
import org.neo4j.cypher.internal.util.symbols.NothingType;
import org.neo4j.cypher.internal.util.symbols.NullType;
import org.neo4j.cypher.internal.util.symbols.NumberType;
import org.neo4j.cypher.internal.util.symbols.PathType;
import org.neo4j.cypher.internal.util.symbols.PointType;
import org.neo4j.cypher.internal.util.symbols.PropertyValueType;
import org.neo4j.cypher.internal.util.symbols.RelationshipType;
import org.neo4j.cypher.internal.util.symbols.StringType;
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType;
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeIdReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathReference;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings({"ReferenceEquality"})
public final class CypherFunctions {
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final String[] POINT_KEYS =
            new String[] {"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

    private CypherFunctions() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    public static AnyValue sin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue) {
            return doubleValue(Math.sin(((NumberValue) in).doubleValue()));
        } else {
            throw needsNumbers("sin()");
        }
    }

    public static AnyValue asin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.asin(number.doubleValue()));
        } else {
            throw needsNumbers("asin()");
        }
    }

    public static AnyValue haversin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue((1.0 - Math.cos(number.doubleValue())) / 2);
        } else {
            throw needsNumbers("haversin()");
        }
    }

    public static AnyValue cos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.cos(number.doubleValue()));
        } else {
            throw needsNumbers("cos()");
        }
    }

    public static AnyValue cot(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(1.0 / Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("cot()");
        }
    }

    public static AnyValue acos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.acos(number.doubleValue()));
        } else {
            throw needsNumbers("acos()");
        }
    }

    public static AnyValue tan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("tan()");
        }
    }

    public static AnyValue atan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.atan(number.doubleValue()));
        } else {
            throw needsNumbers("atan()");
        }
    }

    public static AnyValue atan2(AnyValue y, AnyValue x) {
        if (y == NO_VALUE || x == NO_VALUE) {
            return NO_VALUE;
        } else if (y instanceof NumberValue yNumber && x instanceof NumberValue xNumber) {
            return doubleValue(Math.atan2(yNumber.doubleValue(), xNumber.doubleValue()));
        } else {
            throw needsNumbers("atan2()");
        }
    }

    public static AnyValue ceil(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.ceil(number.doubleValue()));
        } else {
            throw needsNumbers("ceil()");
        }
    }

    public static AnyValue floor(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.floor(number.doubleValue()));
        } else {
            throw needsNumbers("floor()");
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue round(AnyValue in) {
        return round(in, Values.ZERO_INT, Values.stringValue("HALF_UP"), Values.booleanValue(false));
    }

    @CalledFromGeneratedCode
    public static AnyValue round(AnyValue in, AnyValue precision) {
        return round(in, precision, Values.stringValue("HALF_UP"), Values.booleanValue(false));
    }

    public static AnyValue round(AnyValue in, AnyValue precisionValue, AnyValue modeValue) {
        return round(in, precisionValue, modeValue, Values.booleanValue(true));
    }

    public static AnyValue round(AnyValue in, AnyValue precisionValue, AnyValue modeValue, AnyValue explicitModeValue) {
        if (in == NO_VALUE || precisionValue == NO_VALUE || modeValue == NO_VALUE) {
            return NO_VALUE;
        } else if (!(modeValue instanceof StringValue)) {
            throw notAModeString("round", modeValue);
        }

        RoundingMode mode;
        try {
            mode = RoundingMode.valueOf(((StringValue) modeValue).stringValue());
        } catch (IllegalArgumentException e) {
            throw new InvalidArgumentException(
                    "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.");
        }

        if (in instanceof NumberValue inNumber && precisionValue instanceof NumberValue) {
            int precision = asIntExact(precisionValue, () -> "Invalid input for precision value in function 'round()'");
            boolean explicitMode = ((BooleanValue) explicitModeValue).booleanValue();

            if (precision < 0) {
                throw new InvalidArgumentException("Precision argument to 'round()' cannot be negative");
            } else {
                double value = inNumber.doubleValue();
                if (Double.isInfinite(value) || Double.isNaN(value)) {
                    return doubleValue(value);
                }
                /*
                 * For precision zero and no explicit rounding mode, we want to fall back to Java Math.round().
                 * This rounds towards the nearest integer and if there is a tie, towards positive infinity,
                 * which doesn't correspond to any of the rounding modes.
                 */
                else if (precision == 0 && !explicitMode) {
                    return doubleValue(Math.round(value));
                } else {
                    BigDecimal bigDecimal = BigDecimal.valueOf(value);
                    int newScale = Math.min(bigDecimal.scale(), precision);
                    return doubleValue(bigDecimal.setScale(newScale, mode).doubleValue());
                }
            }
        } else {
            throw needsNumbers("round()");
        }
    }

    public static AnyValue abs(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            if (in instanceof IntegralValue) {
                return longValue(Math.abs(number.longValue()));
            } else {
                return doubleValue(Math.abs(number.doubleValue()));
            }
        } else {
            throw needsNumbers("abs()");
        }
    }

    public static AnyValue isNaN(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof FloatingPointValue f) {
            return booleanValue(f.isNaN());
        } else if (in instanceof NumberValue) {
            return BooleanValue.FALSE;
        } else {
            throw needsNumbers("isNaN()");
        }
    }

    public static AnyValue toDegrees(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toDegrees(number.doubleValue()));
        } else {
            throw needsNumbers("toDegrees()");
        }
    }

    public static AnyValue exp(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.exp(number.doubleValue()));
        } else {
            throw needsNumbers("exp()");
        }
    }

    public static AnyValue log(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log(number.doubleValue()));
        } else {
            throw needsNumbers("log()");
        }
    }

    public static AnyValue log10(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log10(number.doubleValue()));
        } else {
            throw needsNumbers("log10()");
        }
    }

    public static AnyValue toRadians(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toRadians(number.doubleValue()));
        } else {
            throw needsNumbers("toRadians()");
        }
    }

    @CalledFromGeneratedCode
    public static ListValue range(AnyValue startValue, AnyValue endValue) {
        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'"),
                1L);
    }

    public static ListValue range(AnyValue startValue, AnyValue endValue, AnyValue stepValue) {
        long step = asLong(stepValue, () -> "Invalid input for step value in function 'range()'");
        if (step == 0L) {
            throw new InvalidArgumentException("Step argument to 'range()' cannot be zero");
        }

        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'"),
                step);
    }

    @CalledFromGeneratedCode
    public static AnyValue signum(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return longValue((long) Math.signum(number.doubleValue()));
        } else {
            throw needsNumbers("signum()");
        }
    }

    public static AnyValue sqrt(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.sqrt(number.doubleValue()));
        } else {
            throw needsNumbers("sqrt()");
        }
    }

    public static DoubleValue rand() {
        return doubleValue(ThreadLocalRandom.current().nextDouble());
    }

    public static TextValue randomUuid() {
        return stringValue(UUID.randomUUID().toString());
    }

    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    public static Value distance(AnyValue lhs, AnyValue rhs) {
        if (lhs instanceof PointValue && rhs instanceof PointValue) {
            return calculateDistance((PointValue) lhs, (PointValue) rhs);
        } else {
            return NO_VALUE;
        }
    }

    public static Value withinBBox(AnyValue point, AnyValue lowerLeft, AnyValue upperRight) {
        if (point instanceof PointValue && lowerLeft instanceof PointValue && upperRight instanceof PointValue) {
            return withinBBox((PointValue) point, (PointValue) lowerLeft, (PointValue) upperRight);
        } else {
            return NO_VALUE;
        }
    }

    public static Value withinBBox(PointValue point, PointValue lowerLeft, PointValue upperRight) {
        CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
        if (crs.equals(lowerLeft.getCoordinateReferenceSystem())
                && crs.equals(upperRight.getCoordinateReferenceSystem())) {
            return Values.booleanValue(crs.getCalculator().withinBBox(point, lowerLeft, upperRight));
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue startNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return startNode(rel, access, cursor);
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'startNode()': Expected %s to be a RelationshipValue", anyValue));
        }
    }

    public static VirtualNodeValue startNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.startNodeId(consumer(access, cursor)));
    }

    public static AnyValue endNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return endNode(rel, access, cursor);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'endNode()': Expected %s to be a RelationshipValue", anyValue));
        }
    }

    public static VirtualNodeValue endNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.endNodeId(consumer(access, cursor)));
    }

    @CalledFromGeneratedCode
    public static VirtualNodeValue otherNode(
            AnyValue anyValue, DbAccess access, VirtualNodeValue node, RelationshipScanCursor cursor) {
        // This is not a function exposed to the user
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (anyValue instanceof VirtualRelationshipValue rel) {
            return otherNode(rel, access, node, cursor);
        } else {
            throw new CypherTypeException(format("Expected %s to be a RelationshipValue", anyValue));
        }
    }

    public static VirtualNodeValue otherNode(
            VirtualRelationshipValue relationship,
            DbAccess access,
            VirtualNodeValue node,
            RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.otherNodeId(node.id(), consumer(access, cursor)));
    }

    @CalledFromGeneratedCode
    public static AnyValue propertyGet(
            String key,
            AnyValue container,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), dbAccess.propertyKey(key), nodeCursor, propertyCursor, true);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperty(
                    rel, dbAccess.propertyKey(key), relationshipScanCursor, propertyCursor, true);
        } else if (container instanceof MapValue map) {
            return map.get(key);
        } else if (container instanceof TemporalValue<?, ?> temporal) {
            return temporal.get(key);
        } else if (container instanceof DurationValue duration) {
            return duration.get(key);
        } else if (container instanceof PointValue point) {
            return point.get(key);
        } else {
            throw new CypherTypeException(format("Type mismatch: expected a map but was %s", container));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue[] propertiesGet(
            String[] keys,
            AnyValue container,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperties(node.id(), propertyKeys(keys, dbAccess), nodeCursor, propertyCursor);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperties(
                    rel, propertyKeys(keys, dbAccess), relationshipScanCursor, propertyCursor);
        } else {
            return CursorUtils.propertiesGet(keys, container);
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue containerIndex(
            AnyValue container,
            AnyValue index,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor, true);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperty(
                    rel, propertyKeyId(dbAccess, index), relationshipScanCursor, propertyCursor, true);
        }
        if (container instanceof MapValue map) {
            return mapAccess(map, index);
        } else if (container instanceof SequenceValue seq) {
            return listAccess(seq, index);
        } else {
            throw new CypherTypeException(format(
                    "`%s` is not a collection or a map. Element access is only possible by performing a collection "
                            + "lookup using an integer index, or by performing a map lookup using a string key (found: %s[%s])",
                    container, container, index));
        }
    }

    @CalledFromGeneratedCode
    public static Value containerIndexExists(
            AnyValue container,
            AnyValue index,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return booleanValue(
                    dbAccess.nodeHasProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor));
        } else if (container instanceof VirtualRelationshipValue rel) {
            return booleanValue(dbAccess.relationshipHasProperty(
                    rel, propertyKeyId(dbAccess, index), relationshipScanCursor, propertyCursor));
        }
        if (container instanceof MapValue map) {
            return booleanValue(map.containsKey(asString(
                    index,
                    () ->
                            // this string assumes that the asString method fails and gives context which
                            // operation went wrong
                            "Cannot use non string value as or in map keys. It was " + index.toString())));
        } else {
            throw new CypherTypeException(format(
                    "`%s` is not a map. Element access is only possible by performing a collection "
                            + "lookup by performing a map lookup using a string key (found: %s[%s])",
                    container, container, index));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue head(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof SequenceValue sequence) {
            if (sequence.length() == 0) {
                return NO_VALUE;
            }

            return sequence.value(0);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'head()': Expected %s to be a list", container));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue tail(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof ListValue) {
            return ((ListValue) container).tail();
        } else if (container instanceof ArrayValue) {
            return VirtualValues.fromArray((ArrayValue) container).tail();
        } else {
            return EMPTY_LIST;
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue last(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        }
        if (container instanceof SequenceValue sequence) {
            int length = sequence.length();
            if (length == 0) {
                return NO_VALUE;
            }

            return sequence.value(length - 1);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'last()': Expected %s to be a list", container));
        }
    }

    public static AnyValue left(AnyValue in, AnyValue endPos) {
        if (in == NO_VALUE || endPos == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            final long len = asLong(endPos, () -> "Invalid input for length value in function 'left()'");
            return text.substring(0, (int) Math.min(len, Integer.MAX_VALUE));
        } else {
            throw notAString("left", in);
        }
    }

    public static AnyValue ltrim(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).ltrim();
        } else {
            throw notAString("ltrim", in);
        }
    }

    public static AnyValue rtrim(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).rtrim();
        } else {
            throw notAString("rtrim", in);
        }
    }

    public static AnyValue trim(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).trim();
        } else {
            throw notAString("trim", in);
        }
    }

    public static AnyValue replace(AnyValue original, AnyValue search, AnyValue replaceWith) {
        if (original == NO_VALUE || search == NO_VALUE || replaceWith == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue) {
            return ((TextValue) original).replace(asString(search), asString(replaceWith));
        } else {
            throw notAString("replace", original);
        }
    }

    public static AnyValue reverse(AnyValue original) {
        if (original == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue text) {
            return text.reverse();
        } else if (original instanceof ListValue list) {
            return list.reverse();
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'reverse()': "
                            + "Expected a string or a list; consider converting the value to a string with toString() or creating a list.");
        }
    }

    public static AnyValue right(AnyValue original, AnyValue length) {
        if (original == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            final long len = asLong(length, () -> "Invalid input for length value in function 'right()'");
            if (len < 0) {
                throw new IndexOutOfBoundsException("negative length");
            }
            final long startVal = asText.length() - len;
            return asText.substring((int) Math.max(0, startVal));
        } else {
            throw notAString("right", original);
        }
    }

    public static AnyValue normalize(AnyValue input) {
        return normalize(input, Values.stringValue("NFC"));
    }

    public static AnyValue normalize(AnyValue input, AnyValue normalForm) {
        if (input == NO_VALUE || normalForm == NO_VALUE) {
            return NO_VALUE;
        }

        Normalizer.Form form;
        try {
            form = Normalizer.Form.valueOf(asTextValue(normalForm).stringValue());
        } catch (IllegalArgumentException e) {
            throw InvalidArgumentException.unknownNormalForm();
        }

        String normalized = Normalizer.normalize(asTextValue(input).stringValue(), form);
        return Values.stringValue(normalized);
    }

    public static AnyValue split(AnyValue original, AnyValue separator) {
        if (original == NO_VALUE || separator == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            if (asText.length() == 0) {
                return VirtualValues.list(EMPTY_STRING);
            }
            if (separator instanceof SequenceValue separatorList) {
                var separators = new ArrayList<String>();
                for (var s : separatorList) {
                    if (s == NO_VALUE) {
                        return NO_VALUE;
                    }
                    separators.add(asString(s));
                }
                return asText.split(separators);
            } else {
                return asText.split(asString(separator));
            }
        } else {
            throw notAString("split", original);
        }
    }

    public static AnyValue substring(AnyValue original, AnyValue start) {
        if (original == NO_VALUE || start == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {

            return asText.substring(asIntExact(start, () -> "Invalid input for start value in function 'substring()'"));
        } else {
            throw notAString("substring", original);
        }
    }

    public static AnyValue substring(AnyValue original, AnyValue start, AnyValue length) {
        if (original == NO_VALUE || start == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {

            return asText.substring(
                    asIntExact(start, () -> "Invalid input for start value in function 'substring()'"),
                    asIntExact(length, () -> "Invalid input for length value in function 'substring()'"));
        } else {
            throw notAString("substring", original);
        }
    }

    public static AnyValue toLower(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).toLower();
        } else {
            throw notAString("toLower", in);
        }
    }

    public static AnyValue toUpper(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).toUpper();
        } else {
            throw notAString("toUpper", in);
        }
    }

    public static Value id(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualNodeValue) {
            return longValue(((VirtualNodeValue) item).id());
        } else if (item instanceof VirtualRelationshipValue) {
            return longValue(((VirtualRelationshipValue) item).id());
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'id()': Expected %s to be a node or relationship, but it was `%s`",
                    item, item.getTypeName()));
        }
    }

    public static AnyValue elementId(AnyValue entity, ElementIdMapper idMapper) {
        if (entity == NO_VALUE) {
            return NO_VALUE;
        } else if (entity instanceof NodeValue node) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(node.elementId());
        } else if (entity instanceof VirtualNodeValue node) {
            return stringValue(idMapper.nodeElementId(node.id()));
        } else if (entity instanceof RelationshipValue relationship) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(relationship.elementId());
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return stringValue(idMapper.relationshipElementId(relationship.id()));
        }

        throw new CypherTypeException(format(
                "Invalid input for function 'elementId()': Expected %s to be a node or relationship, but it was `%s`",
                entity, entity.getTypeName()));
    }

    public static AnyValue elementIdToNodeId(AnyValue elementId, ElementIdMapper idMapper) {
        if (elementId == NO_VALUE) {
            return NO_VALUE;
        } else if (elementId instanceof TextValue str) {
            try {
                return longValue(idMapper.nodeId(str.stringValue()));
            } catch (IllegalArgumentException e) {
                return NO_VALUE;
            }
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdToRelationshipId(AnyValue elementId, ElementIdMapper idMapper) {
        if (elementId == NO_VALUE) {
            return NO_VALUE;
        } else if (elementId instanceof TextValue str) {
            try {
                return longValue(idMapper.relationshipId(str.stringValue()));
            } catch (IllegalArgumentException e) {
                return NO_VALUE;
            }
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdListToNodeIdList(AnyValue collection, ElementIdMapper idMapper) {
        if (collection == NO_VALUE) {
            return NO_VALUE;
        } else if (collection instanceof SequenceValue elementIds) {
            var builder = ListValueBuilder.newListBuilder(elementIds.length());
            for (var elementId : elementIds) {
                AnyValue value = elementIdToNodeId(elementId, idMapper);
                builder.add(value);
            }
            return builder.build();
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdListToRelationshipIdList(AnyValue collection, ElementIdMapper idMapper) {
        if (collection == NO_VALUE) {
            return NO_VALUE;
        } else if (collection instanceof SequenceValue elementIds) {
            var builder = ListValueBuilder.newListBuilder(elementIds.length());
            for (var elementId : elementIds) {
                AnyValue value = elementIdToRelationshipId(elementId, idMapper);
                builder.add(value);
            }
            return builder.build();
        }
        return NO_VALUE;
    }

    public static TextValue nodeElementId(long id, ElementIdMapper idMapper) {
        assert id > NO_ID;
        return Values.stringValue(idMapper.nodeElementId(id));
    }

    public static TextValue relationshipElementId(long id, ElementIdMapper idMapper) {
        assert id > NO_ID;
        return Values.stringValue(idMapper.relationshipElementId(id));
    }

    public static AnyValue labels(AnyValue item, DbAccess access, NodeCursor nodeCursor) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualNodeValue node) {
            return access.getLabelsForNode(node.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Invalid input for function 'labels()': Expected a Node, got: " + item);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabel(AnyValue entity, int labelToken, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isLabelSetOnNode(labelToken, node.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Expected a Node, got: " + entity);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabels(AnyValue entity, int[] labelTokens, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.areLabelsSetOnNode(labelTokens, node.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Expected a Node, got: " + entity);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasALabel(AnyValue entity, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue virtualNodeValue) {
            return access.isALabelSetOnNode(virtualNodeValue.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Expected a Node, got: " + entity);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasALabelOrType(AnyValue entity, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isALabelSetOnNode(node.id(), nodeCursor);
        } else if (entity instanceof VirtualRelationshipValue) {
            return true;
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasALabelOrType()': Expected %s to be a node or relationship, but it was `%s`",
                    entity, entity.getTypeName()));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabelsOrTypes(
            AnyValue entity,
            DbAccess access,
            int[] labels,
            NodeCursor nodeCursor,
            int[] types,
            RelationshipScanCursor relationshipScanCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.areLabelsSetOnNode(labels, node.id(), nodeCursor);
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return access.areTypesSetOnRelationship(types, relationship, relationshipScanCursor);
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasALabelOrType()': Expected %s to be a node or relationship, but it was `%s`",
                    entity, entity.getTypeName()));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyLabel(AnyValue entity, int[] labels, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isAnyLabelSetOnNode(labels, node.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Expected a Node, got: " + entity);
        }
    }

    public static AnyValue type(AnyValue item, DbAccess access, RelationshipScanCursor relCursor, Read read) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof RelationshipValue relationship) {
            return relationship.type();
        } else if (item instanceof VirtualRelationshipValue relationship) {

            int typeToken = relationship.relationshipTypeId(relationshipVisitor -> {
                long relationshipId = relationshipVisitor.id();
                access.singleRelationship(relationshipId, relCursor);

                if (relCursor.next() || read.relationshipDeletedInTransaction(relationshipId)) {
                    relationshipVisitor.visit(
                            relCursor.sourceNodeReference(), relCursor.targetNodeReference(), relCursor.type());
                }
            });

            if (typeToken == TokenConstants.NO_TOKEN) {
                return NO_VALUE;
            } else {
                return Values.stringValue(access.relationshipTypeName(typeToken));
            }
        } else {
            throw new CypherTypeException("Invalid input for function 'type()': Expected a Relationship, got: " + item);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasType(AnyValue entity, int typeToken, DbAccess access, RelationshipScanCursor relCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            if (typeToken == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                return false;
            } else {
                return typeToken == relationship.relationshipTypeId(consumer(access, relCursor));
            }
        } else {
            throw new CypherTypeException("Expected a Relationship, got: " + entity);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasTypes(
            AnyValue entity, int[] typeTokens, DbAccess access, RelationshipScanCursor relCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            return access.areTypesSetOnRelationship(typeTokens, relationship, relCursor);
        } else {
            throw new CypherTypeException("Expected a Relationship, got: " + entity);
        }
    }

    public static AnyValue nodes(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof PathValue) {
            return VirtualValues.list(((PathValue) in).nodes());
        } else if (in instanceof VirtualPathValue) {
            long[] ids = ((VirtualPathValue) in).nodeIds();
            ListValueBuilder builder = ListValueBuilder.newListBuilder(ids.length);
            for (long id : ids) {
                builder.add(VirtualValues.node(id));
            }
            return builder.build();
        } else {
            throw new CypherTypeException(format("Invalid input for function 'nodes()': Expected %s to be a path", in));
        }
    }

    public static AnyValue relationships(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualPathValue path) {
            return path.relationshipsAsList();
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'relationships()': Expected %s to be a path", in));
        }
    }

    public static Value point(AnyValue in, DbAccess access, ExpressionCursors cursors) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return asPoint(access, node, cursors.nodeCursor(), cursors.propertyCursor());
        } else if (in instanceof VirtualRelationshipValue rel) {
            return asPoint(access, rel, cursors.relationshipScanCursor(), cursors.propertyCursor());
        } else if (in instanceof MapValue map) {
            if (containsNull(map)) {
                return NO_VALUE;
            }
            return PointValue.fromMap(map);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'point()': Expected a map but got %s", in));
        }
    }

    public static AnyValue keys(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return extractKeys(access, access.nodePropertyIds(node.id(), nodeCursor, propertyCursor));
        } else if (in instanceof VirtualRelationshipValue rel) {
            return extractKeys(access, access.relationshipPropertyIds(rel, relationshipScanCursor, propertyCursor));
        } else if (in instanceof MapValue) {
            return ((MapValue) in).keys();
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'keys()': Expected a node, a relationship or a literal map but got %s",
                    in));
        }
    }

    public static AnyValue properties(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return access.nodeAsMap(node.id(), nodeCursor, propertyCursor);
        } else if (in instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(rel, relationshipCursor, propertyCursor);
        } else if (in instanceof MapValue) {
            return in;
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s",
                    in));
        }
    }

    public static AnyValue characterLength(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue) {
            return size(item);
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'character_length()': Expected a String, got: " + item);
        }
    }

    public static AnyValue size(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue) {
            return longValue(((TextValue) item).length());
        } else if (item instanceof SequenceValue) {
            return longValue(((SequenceValue) item).length());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'size()': Expected a String or List, got: " + item);
        }
    }

    public static AnyValue isEmpty(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof SequenceValue) {
            return Values.booleanValue(((SequenceValue) item).isEmpty());
        } else if (item instanceof MapValue) {
            return Values.booleanValue(((MapValue) item).isEmpty());
        } else if (item instanceof TextValue) {
            return Values.booleanValue(((TextValue) item).isEmpty());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'isEmpty()': Expected a List, Map, or String, got: " + item);
        }
    }

    public static AnyValue length(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualPathValue) {
            return longValue(((VirtualPathValue) item).size());
        } else {
            throw new CypherTypeException("Invalid input for function 'length()': Expected a Path, got: " + item);
        }
    }

    public static Value toBoolean(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof BooleanValue) {
            return (BooleanValue) in;
        } else if (in instanceof TextValue) {
            return switch (((TextValue) in).trim().stringValue().toLowerCase(Locale.ROOT)) {
                case "true" -> TRUE;
                case "false" -> FALSE;
                default -> NO_VALUE;
            };
        } else if (in instanceof IntegralValue integer) {
            return integer.longValue() == 0L ? FALSE : TRUE;
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toBoolean()': Expected a Boolean, Integer or String, got: " + in);
        }
    }

    public static Value toBooleanOrNull(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof BooleanValue || in instanceof TextValue || in instanceof IntegralValue) {
            return toBoolean(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toBooleanList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toBooleanOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toBooleanList()': Expected a List, got: %s", in));
        }
    }

    public static Value toFloat(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof DoubleValue) {
            return (DoubleValue) in;
        } else if (in instanceof NumberValue number) {
            return doubleValue(number.doubleValue());
        } else if (in instanceof TextValue) {
            try {
                return doubleValue(parseDouble(((TextValue) in).stringValue()));
            } catch (NumberFormatException ignore) {
                return NO_VALUE;
            }
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: " + in);
        }
    }

    public static Value toFloatOrNull(AnyValue in) {
        if (in instanceof NumberValue || in instanceof TextValue) {
            return toFloat(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toFloatList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toFloatOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toFloatList()': Expected a List, got: %s", in));
        }
    }

    public static Value toInteger(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralValue) {
            return (IntegralValue) in;
        } else if (in instanceof NumberValue number) {
            return longValue(number.longValue());
        } else if (in instanceof TextValue) {
            return stringToLongValue((TextValue) in);
        } else if (in instanceof BooleanValue) {
            if (((BooleanValue) in).booleanValue()) {
                return longValue(1L);
            } else {
                return longValue(0L);
            }
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toInteger()': Expected a String, Float, Integer or Boolean, got: "
                            + in);
        }
    }

    public static Value toIntegerOrNull(AnyValue in) {
        if (in instanceof NumberValue || in instanceof BooleanValue) {
            return toInteger(in);
        } else if (in instanceof TextValue) {
            try {
                return stringToLongValue((TextValue) in);
            } catch (CypherTypeException e) {
                return NO_VALUE;
            }
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toIntegerList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralArray array) {
            return VirtualValues.fromArray(array);
        } else if (in instanceof FloatingPointArray array) {
            return toIntegerList(array);
        } else if (in instanceof SequenceValue sequence) {
            return toIntegerList(sequence);
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toIntegerList()': Expected a List, got: %s", in));
        }
    }

    public static Value toString(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            return text;
        } else if (in instanceof NumberValue number) {
            return stringValue(number.prettyPrint());
        } else if (in instanceof BooleanValue b) {
            return stringValue(b.prettyPrint());
        } else if (in instanceof TemporalValue || in instanceof DurationValue || in instanceof PointValue) {
            return stringValue(in.toString());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toString()': Expected a String, Float, Integer, Boolean, Temporal or Duration, got: "
                            + in);
        }
    }

    public static AnyValue toStringOrNull(AnyValue in) {
        if (in instanceof TextValue
                || in instanceof NumberValue
                || in instanceof BooleanValue
                || in instanceof TemporalValue
                || in instanceof DurationValue
                || in instanceof PointValue) {
            return toString(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toStringList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toStringOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toStringList()': Expected a List, got: %s", in));
        }
    }

    public static AnyValue fromSlice(AnyValue collection, AnyValue fromValue) {
        if (collection == NO_VALUE || fromValue == NO_VALUE) {
            return NO_VALUE;
        }

        int from = asIntExact(fromValue);
        ListValue list = asList(collection);
        if (from >= 0) {
            return list.drop(from);
        } else {
            return list.drop(list.size() + from);
        }
    }

    public static AnyValue toSlice(AnyValue collection, AnyValue toValue) {
        if (collection == NO_VALUE || toValue == NO_VALUE) {
            return NO_VALUE;
        }
        int from = asIntExact(toValue);
        ListValue list = asList(collection);
        if (from >= 0) {
            return list.take(from);
        } else {
            return list.take(list.size() + from);
        }
    }

    public static AnyValue fullSlice(AnyValue collection, AnyValue fromValue, AnyValue toValue) {
        if (collection == NO_VALUE || fromValue == NO_VALUE || toValue == NO_VALUE) {
            return NO_VALUE;
        }
        int from = asIntExact(fromValue);
        int to = asIntExact(toValue);
        ListValue list = asList(collection);
        int size = list.size();
        if (from >= 0 && to >= 0) {
            return list.slice(from, to);
        } else if (from >= 0) {
            return list.slice(from, size + to);
        } else if (to >= 0) {
            return list.slice(size + from, to);
        } else {
            return list.slice(size + from, size + to);
        }
    }

    @CalledFromGeneratedCode
    public static TextValue asTextValue(AnyValue value) {
        return asTextValue(value, null);
    }

    public static TextValue asTextValue(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (!(value instanceof TextValue)) {
            String errorMessage;
            if (contextForErrorMessage == null) {
                errorMessage = format(
                        "Expected %s to be a %s, but it was a %s",
                        value, TextValue.class.getName(), value.getClass().getName());
            } else {
                errorMessage = format(
                        "%s: Expected %s to be a %s, but it was a %s",
                        contextForErrorMessage.get(),
                        value,
                        TextValue.class.getName(),
                        value.getClass().getName());
            }

            throw new CypherTypeException(errorMessage);
        }
        return (TextValue) value;
    }

    private static Value stringToLongValue(TextValue in) {
        try {
            return longValue(parseLong(in.stringValue()));
        } catch (Exception e) {
            try {
                BigDecimal bigDecimal = new BigDecimal(in.stringValue());
                if (bigDecimal.compareTo(MAX_LONG) <= 0 && bigDecimal.compareTo(MIN_LONG) >= 0) {
                    return longValue(bigDecimal.longValue());
                } else {
                    throw new CypherTypeException(format("integer, %s, is too large", in.stringValue()));
                }
            } catch (NumberFormatException ignore) {
                return NO_VALUE;
            }
        }
    }

    private static ListValue extractKeys(DbAccess access, int[] keyIds) {
        String[] keysNames = new String[keyIds.length];
        for (int i = 0; i < keyIds.length; i++) {
            keysNames[i] = access.getPropertyKeyName(keyIds[i]);
        }
        return VirtualValues.fromArray(Values.stringArray(keysNames));
    }

    private static Value asPoint(
            DbAccess access, VirtualNodeValue nodeValue, NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        MapValueBuilder builder = new MapValueBuilder();
        for (String key : POINT_KEYS) {
            Value value =
                    access.nodeProperty(nodeValue.id(), access.propertyKey(key), nodeCursor, propertyCursor, true);
            if (value == NO_VALUE) {
                continue;
            }
            builder.add(key, value);
        }

        return PointValue.fromMap(builder.build());
    }

    private static Value asPoint(
            DbAccess access,
            VirtualRelationshipValue relationshipValue,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        MapValueBuilder builder = new MapValueBuilder();
        for (String key : POINT_KEYS) {
            Value value = access.relationshipProperty(
                    relationshipValue, access.propertyKey(key), relationshipScanCursor, propertyCursor, true);
            if (value == NO_VALUE) {
                continue;
            }
            builder.add(key, value);
        }

        return PointValue.fromMap(builder.build());
    }

    private static boolean containsNull(MapValue map) {
        boolean[] hasNull = {false};
        map.foreach((s, value) -> {
            if (value == NO_VALUE) {
                hasNull[0] = true;
            }
        });
        return hasNull[0];
    }

    private static AnyValue listAccess(SequenceValue container, AnyValue index) {
        NumberValue number = asNumberValue(
                index,
                () -> "Cannot access a list '" + container.toString() + "' using a non-number index, got "
                        + index.toString());
        if (!(number instanceof IntegralValue)) {
            throw new CypherTypeException(
                    format("Cannot access a list using an non-integer number index, got %s", number), null);
        }
        long idx = number.longValue();
        if (idx > Integer.MAX_VALUE || idx < Integer.MIN_VALUE) {
            throw new InvalidArgumentException(format(
                    "Cannot index a list using a value greater than %d or lesser than %d, got %d",
                    Integer.MAX_VALUE, Integer.MIN_VALUE, idx));
        }

        if (idx < 0) {
            idx = container.length() + idx;
        }
        if (idx >= container.length() || idx < 0) {
            return NO_VALUE;
        }
        return container.value((int) idx);
    }

    private static int propertyKeyId(DbAccess dbAccess, AnyValue index) {
        return dbAccess.propertyKey(asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot use a property key with non string name. It was " + index.toString()));
    }

    private static AnyValue mapAccess(MapValue container, AnyValue index) {
        return container.get(asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot access a map '" + container.toString() + "' by key '" + index.toString() + "'"));
    }

    private static String asString(AnyValue value) {
        return asTextValue(value).stringValue();
    }

    private static String asString(AnyValue value, Supplier<String> contextForErrorMessage) {
        return asTextValue(value, contextForErrorMessage).stringValue();
    }

    private static NumberValue asNumberValue(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (!(value instanceof NumberValue)) {
            throw new CypherTypeException(format(
                    "%s: Expected %s to be a %s, but it was a %s",
                    contextForErrorMessage.get(),
                    value,
                    NumberValue.class.getName(),
                    value.getClass().getName()));
        }
        return (NumberValue) value;
    }

    private static Value calculateDistance(PointValue p1, PointValue p2) {
        if (p1.getCoordinateReferenceSystem().equals(p2.getCoordinateReferenceSystem())) {
            return doubleValue(p1.getCoordinateReferenceSystem().getCalculator().distance(p1, p2));
        } else {
            return NO_VALUE;
        }
    }

    private static long asLong(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (value instanceof NumberValue) {
            return ((NumberValue) value).longValue();
        } else {
            String errorMsg;
            if (contextForErrorMessage == null) {
                errorMsg = "Expected a numeric value but got: " + value;
            } else {
                errorMsg = contextForErrorMessage.get() + ": Expected a numeric value but got: " + value;
            }
            throw new CypherTypeException(errorMsg);
        }
    }

    public static int asIntExact(AnyValue value) {
        return asIntExact(value, null);
    }

    public static int asIntExact(AnyValue value, Supplier<String> contextForErrorMessage) {
        final long longValue = asLong(value, contextForErrorMessage);
        final int intValue = (int) longValue;
        if (intValue != longValue) {
            String errorMsg = format(
                    "Expected an integer between %d and %d, but got: %d",
                    Integer.MIN_VALUE, Integer.MAX_VALUE, longValue);
            if (contextForErrorMessage != null) {
                errorMsg = contextForErrorMessage.get() + ": " + errorMsg;
            }
            throw new IllegalArgumentException(errorMsg);
        }
        return intValue;
    }

    public static long nodeId(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (value instanceof VirtualNodeValue) {
            return ((VirtualNodeValue) value).id();
        } else {
            throw new CypherTypeException(
                    "Expected VirtualNodeValue got " + value.getClass().getName());
        }
    }

    public static AnyValue isNormalized(AnyValue input, NormalForm normalForm) {
        if (input == NO_VALUE) {
            return NO_VALUE;
        }

        if (input instanceof TextValue asText) {
            Normalizer.Form form;
            try {
                form = Normalizer.Form.valueOf(normalForm.description());
            } catch (IllegalArgumentException e) {
                throw InvalidArgumentException.unknownNormalForm();
            }
            boolean normalized = Normalizer.isNormalized(asText.stringValue(), form);
            return Values.booleanValue(normalized);
        } else {
            return NO_VALUE;
        }
    }

    public static BooleanValue isTyped(AnyValue item, CypherType typeName) {
        boolean result;
        if (typeName instanceof NothingType) {
            result = false;
        } else if (item instanceof NoValue) {
            result = typeName instanceof NullType || typeName.isNullable();
        } else if (typeName instanceof NullType) {
            result = false;
        } else if (typeName instanceof AnyType) {
            result = true;
        } else if (typeName instanceof ListType listType) {
            result = (item instanceof SequenceValue list) && checkInnerListIsTyped(list, listType);
        } else if (typeName.hasValueRepresentation()) {
            result = possibleValueRepresentations(typeName).contains(item.valueRepresentation());
        } else if (typeName instanceof NodeType) {
            result = item instanceof NodeIdReference;
        } else if (typeName instanceof RelationshipType) {
            result = item instanceof RelationshipReference;
        } else if (typeName instanceof MapType) {
            result = item instanceof MapValue;
        } else if (typeName instanceof PathType) {
            result = item instanceof PathReference;
        } else if (typeName instanceof PropertyValueType) {
            result = hasPropertyValueRepresentation(item.valueRepresentation())
                    || (item instanceof ListValue listValue
                            && (listValue.isEmpty()
                                    || hasPropertyValueRepresentation(listValue.itemValueRepresentation())));
        } else if (typeName instanceof ClosedDynamicUnionType unionType) {
            result = false;
            for (CypherType innerType : asJava(unionType.innerTypes())) {
                if (isTyped(item, innerType) == TRUE) {
                    result = true;
                    break;
                }
            }
        } else {
            throw new IllegalArgumentException(String.format("Unexpected type: %s", typeName.toCypherTypeString()));
        }

        return Values.booleanValue(result);
    }

    private static final CypherTypeValueMapper CYPHER_TYPE_NAME_VALUE_MAPPER = new CypherTypeValueMapper();

    public static Value valueType(AnyValue in) {
        return Values.stringValue(
                CypherType.normalizeTypes(in.map(CYPHER_TYPE_NAME_VALUE_MAPPER)).description());
    }

    private static boolean hasPropertyValueRepresentation(ValueRepresentation valueRepresentation) {
        return !valueRepresentation.equals(ValueRepresentation.ANYTHING)
                && !valueRepresentation.equals(ValueRepresentation.UNKNOWN)
                && !valueRepresentation.equals(ValueRepresentation.NO_VALUE);
    }

    private static List<ValueRepresentation> possibleValueRepresentations(CypherType cypherType)
            throws UnsupportedOperationException {
        if (cypherType instanceof BooleanType) {
            return List.of(ValueRepresentation.BOOLEAN);
        } else if (cypherType instanceof StringType) {
            return List.of(ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT);
        } else if (cypherType instanceof IntegerType) {
            return List.of(
                    ValueRepresentation.INT8,
                    ValueRepresentation.INT16,
                    ValueRepresentation.INT32,
                    ValueRepresentation.INT64);
        } else if (cypherType instanceof FloatType) {
            return List.of(ValueRepresentation.FLOAT32, ValueRepresentation.FLOAT64);
        } else if (cypherType instanceof NumberType) {
            return List.of(
                    ValueRepresentation.INT8,
                    ValueRepresentation.INT16,
                    ValueRepresentation.INT32,
                    ValueRepresentation.INT64,
                    ValueRepresentation.FLOAT32,
                    ValueRepresentation.FLOAT64);
        } else if (cypherType instanceof DateType) {
            return List.of(ValueRepresentation.DATE);
        } else if (cypherType instanceof LocalTimeType) {
            return List.of(ValueRepresentation.LOCAL_TIME);
        } else if (cypherType instanceof ZonedTimeType) {
            return List.of(ValueRepresentation.ZONED_TIME);
        } else if (cypherType instanceof LocalDateTimeType) {
            return List.of(ValueRepresentation.LOCAL_DATE_TIME);
        } else if (cypherType instanceof ZonedDateTimeType) {
            return List.of(ValueRepresentation.ZONED_DATE_TIME);
        } else if (cypherType instanceof DurationType) {
            return List.of(ValueRepresentation.DURATION);
        } else if (cypherType instanceof GeometryType || cypherType instanceof PointType) {
            return List.of(ValueRepresentation.GEOMETRY);
        } else if (cypherType instanceof ListType listType) {
            if (listType.innerType() instanceof BooleanType) {
                return List.of(ValueRepresentation.BOOLEAN_ARRAY);
            } else if (listType.innerType() instanceof StringType) {
                return List.of(ValueRepresentation.TEXT_ARRAY);
            } else if (listType.innerType() instanceof IntegerType) {
                return List.of(
                        ValueRepresentation.INT8_ARRAY,
                        ValueRepresentation.INT16_ARRAY,
                        ValueRepresentation.INT32_ARRAY,
                        ValueRepresentation.INT64_ARRAY);
            } else if (listType.innerType() instanceof FloatType) {
                return List.of(ValueRepresentation.FLOAT32_ARRAY, ValueRepresentation.FLOAT64_ARRAY);
            } else if (listType.innerType() instanceof NumberType) {
                return List.of(
                        ValueRepresentation.INT8_ARRAY,
                        ValueRepresentation.INT16_ARRAY,
                        ValueRepresentation.INT32_ARRAY,
                        ValueRepresentation.INT64_ARRAY,
                        ValueRepresentation.FLOAT32_ARRAY,
                        ValueRepresentation.FLOAT64_ARRAY);
            } else if (listType.innerType() instanceof DateType) {
                return List.of(ValueRepresentation.DATE_ARRAY);
            } else if (listType.innerType() instanceof LocalTimeType) {
                return List.of(ValueRepresentation.LOCAL_TIME_ARRAY);
            } else if (listType.innerType() instanceof ZonedTimeType) {
                return List.of(ValueRepresentation.ZONED_TIME_ARRAY);
            } else if (listType.innerType() instanceof LocalDateTimeType) {
                return List.of(ValueRepresentation.LOCAL_DATE_TIME_ARRAY);
            } else if (listType.innerType() instanceof ZonedDateTimeType) {
                return List.of(ValueRepresentation.ZONED_DATE_TIME_ARRAY);
            } else if (listType.innerType() instanceof DurationType) {
                return List.of(ValueRepresentation.DURATION_ARRAY);
            } else if (listType.innerType() instanceof PointType || listType.innerType() instanceof GeometryType) {
                return List.of(ValueRepresentation.GEOMETRY_ARRAY);
            } else {
                return List.of();
            }
        } else {
            throw new UnsupportedOperationException(String.format(
                    "possibleValueRepresentations not supported on %s",
                    cypherType.getClass().getName()));
        }
    }

    private static boolean checkInnerListIsTyped(SequenceValue values, ListType typeName) {
        final var itemType = typeName.innerType();
        // An empty list can be a list of anything, even NOTHING, so don't check further
        // A list of LIST<ANY> can also be anything, so no need to check further
        if (values.isEmpty() || (!itemType.isNullable() && itemType instanceof AnyType)) return true;
        // A non-empty list of NOTHING is always false
        if (itemType instanceof NothingType) return false;
        if (values instanceof ArrayValue array) {
            // An ArrayValue can only hold storable types (not null)
            // So a LIST<ANY [NOT NULL]>, LIST<PROPERTY VALUE [NOT NULL]> are true
            // else check that the specific array type matches
            return itemType instanceof AnyType
                    || itemType instanceof PropertyValueType
                    || (typeName.hasValueRepresentation()
                            && possibleValueRepresentations(typeName).contains(array.valueRepresentation()));
        } else if (values instanceof ListValue list) {
            // For a simple LIST<TYPE NOT NULL> we can quickly check the list type
            // without needing to iterate over the list
            // Lists that are mixed ints and floats will return as a list of float here, so don't allow the shortcut for
            // that
            if (itemType.hasValueRepresentation()
                    && !itemType.isNullable()
                    && list.itemValueRepresentation().valueGroup() != ValueGroup.NUMBER
                    && possibleValueRepresentations(itemType).contains(list.itemValueRepresentation())) {
                return true;
            } else {
                // The list is either mixed, or may contain nulls, must check all values
                for (AnyValue value : values) {
                    if (isTyped(value, itemType) == FALSE) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static AnyValue assertIsNode(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualNodeValue) {
            return TRUE;
        } else {
            throw new CypherTypeException("Expected a Node, got: " + item);
        }
    }

    private static CypherTypeException needsNumbers(String method) {
        return new CypherTypeException(format("%s requires numbers", method));
    }

    private static CypherTypeException notAString(String method, AnyValue in) {
        return new CypherTypeException(format(
                "Expected a string value for `%s`, but got: %s; consider converting it to a string with "
                        + "toString().",
                method, in));
    }

    private static CypherTypeException notAModeString(String method, AnyValue mode) {
        return new CypherTypeException(format("Expected a string value for `%s`, but got: %s.", method, mode));
    }

    private static ListValue toIntegerList(FloatingPointArray array) {
        var converted = ListValueBuilder.newListBuilder(array.length());
        for (int i = 0; i < array.length(); i++) {
            converted.add(longValue((long) array.doubleValue(i)));
        }
        return converted.build();
    }

    private static ListValue toIntegerList(SequenceValue sequenceValue) {
        var converted = ListValueBuilder.newListBuilder();
        for (AnyValue value : sequenceValue) {
            converted.add(value != NO_VALUE ? toIntegerOrNull(value) : NO_VALUE);
        }
        return converted.build();
    }

    private static Consumer<RelationshipVisitor> consumer(DbAccess access, RelationshipScanCursor cursor) {
        return relationshipVisitor -> {
            access.singleRelationship(relationshipVisitor.id(), cursor);
            if (cursor.next()) {
                relationshipVisitor.visit(cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.type());
            }
        };
    }
}
