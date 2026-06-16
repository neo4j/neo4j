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

import static org.neo4j.cypher.operations.CursorUtils.propertyKeys;
import static org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.ConflictingDynamicTypes;
import static org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.EmptyDynamicTypeList;
import static org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.SingleDynamicType;
import static org.neo4j.cypher.operations.VectorUtils.assertDimension;
import static org.neo4j.cypher.operations.VectorUtils.invalidVector;
import static org.neo4j.internal.kernel.api.TokenWrite.checkValidTokenName;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.NaN;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.asList;
import static org.neo4j.values.virtual.VirtualValues.fromList;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.StreamSupport;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.cypher.internal.expressions.NormalForm;
import org.neo4j.cypher.internal.expressions.functions.VectorSimilarityCosine;
import org.neo4j.cypher.internal.expressions.functions.VectorSimilarityEuclidean;
import org.neo4j.cypher.internal.notification.RuntimeUnsatisfiableRelationshipTypeExpression;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.QueryContext;
import org.neo4j.cypher.internal.runtime.RuntimeNotifier;
import org.neo4j.cypher.internal.runtime.cursors.ExpressionCursors;
import org.neo4j.cypher.internal.util.symbols.AnyType;
import org.neo4j.cypher.internal.util.symbols.BooleanType;
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType;
import org.neo4j.cypher.internal.util.symbols.CypherType;
import org.neo4j.cypher.internal.util.symbols.DateType;
import org.neo4j.cypher.internal.util.symbols.DurationType;
import org.neo4j.cypher.internal.util.symbols.Float32Type;
import org.neo4j.cypher.internal.util.symbols.FloatType;
import org.neo4j.cypher.internal.util.symbols.GeometryType;
import org.neo4j.cypher.internal.util.symbols.Integer16Type;
import org.neo4j.cypher.internal.util.symbols.Integer32Type;
import org.neo4j.cypher.internal.util.symbols.Integer8Type;
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
import org.neo4j.cypher.internal.util.symbols.PropertyValueCypher5Type;
import org.neo4j.cypher.internal.util.symbols.PropertyValueType;
import org.neo4j.cypher.internal.util.symbols.RelationshipType;
import org.neo4j.cypher.internal.util.symbols.StringType;
import org.neo4j.cypher.internal.util.symbols.UUIDType;
import org.neo4j.cypher.internal.util.symbols.VectorType;
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType;
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InternalException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.ParameterWrongTypeException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarity;
import org.neo4j.kernel.api.vector.GQLVectorDistanceFunction;
import org.neo4j.kernel.api.vector.GQLVectorNorm;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenType;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValues;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.VectorCandidate;
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
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.CompositeDatabaseValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings({"ReferenceEquality", "deprecation"})
public final class CypherFunctions {
    private static final String[] POINT_KEYS =
            new String[] {"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

    /*
     * The random number generator used by this class to create random
     * based UUIDs. In a holder class to defer initialization until needed.
     */
    private static class Holder {
        static final SecureRandom numberGenerator = new SecureRandom();
    }

    private CypherFunctions() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    public static AnyValue sin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.sin(number.doubleValue()));
        } else {
            throw needsNumbers("sin", in);
        }
    }

    public static AnyValue sinh(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.sinh(number.doubleValue()));
        } else {
            throw needsNumbers("sinh", in);
        }
    }

    public static AnyValue asin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.asin(number.doubleValue()));
        } else {
            throw needsNumbers("asin", in);
        }
    }

    public static AnyValue haversin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue((1.0 - Math.cos(number.doubleValue())) / 2);
        } else {
            throw needsNumbers("haversin", in);
        }
    }

    public static AnyValue cos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.cos(number.doubleValue()));
        } else {
            throw needsNumbers("cos", in);
        }
    }

    public static AnyValue cosh(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.cosh(number.doubleValue()));
        } else {
            throw needsNumbers("cosh", in);
        }
    }

    public static AnyValue cot(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(1.0 / Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("cot", in);
        }
    }

    public static AnyValue coth(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            double inDouble = number.doubleValue();
            if (Double.isInfinite(inDouble)) {
                if (Math.signum(inDouble) == -1) {
                    return doubleValue(-1.0);
                }
                return doubleValue(1.0);
            } else if (inDouble == 0.0) {
                return NaN;
            }
            return doubleValue(Math.cosh(inDouble) / Math.sinh(inDouble));
        } else {
            throw needsNumbers("coth", in);
        }
    }

    public static AnyValue acos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.acos(number.doubleValue()));
        } else {
            throw needsNumbers("acos", in);
        }
    }

    public static AnyValue tan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("tan", in);
        }
    }

    public static AnyValue tanh(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.tanh(number.doubleValue()));
        } else {
            throw needsNumbers("tanh", in);
        }
    }

    public static AnyValue atan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.atan(number.doubleValue()));
        } else {
            throw needsNumbers("atan", in);
        }
    }

    public static AnyValue atan2(AnyValue y, AnyValue x) {
        if (y == NO_VALUE || x == NO_VALUE) {
            return NO_VALUE;
        } else if (y instanceof NumberValue yNumber && x instanceof NumberValue xNumber) {
            return doubleValue(Math.atan2(yNumber.doubleValue(), xNumber.doubleValue()));
        } else if (y instanceof NumberValue) {
            throw needsNumbers("atan2", x);
        } else {
            throw needsNumbers("atan2", y);
        }
    }

    public static AnyValue ceil(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.ceil(number.doubleValue()));
        } else {
            throw needsNumbers("ceil", in);
        }
    }

    public static AnyValue floor(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.floor(number.doubleValue()));
        } else {
            throw needsNumbers("floor", in);
        }
    }

    public static AnyValue format(AnyValue value) {
        if (value == NO_VALUE) {
            return NO_VALUE;
        } else if (value instanceof TemporalValue || value instanceof DurationValue) {
            return stringValue(value.toString());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'format()': Expected to be Duration, Date, Time, LocalTime, LocalDateTime or DateTime, got: "
                            + value.prettify() + ".",
                    "format",
                    value.prettify(),
                    List.of("DATE", "LOCAL TIME", "ZONED TIME", "LOCAL DATETIME", "ZONED DATETIME", "DURATION"),
                    CypherTypeValueMapper.valueType(value));
        }
    }

    public static AnyValue format(AnyValue value, AnyValue pattern) {
        if (value == NO_VALUE || pattern == NO_VALUE) {
            return NO_VALUE;
        } else if (value instanceof TemporalValue<?, ?> instant && pattern instanceof TextValue patternString) {
            return stringValue(instant.format(patternString.stringValue()));
        } else if (value instanceof DurationValue duration && pattern instanceof TextValue patternString) {
            return stringValue(duration.format(patternString.stringValue()));
        } else if (!(pattern instanceof TextValue)) {
            throw notAString("format", pattern);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'format()': Expected to be Duration, Date, Time, LocalTime, LocalDateTime or DateTime, got: "
                            + value.prettify() + ".",
                    "format",
                    value.prettify(),
                    List.of("DATE", "LOCAL TIME", "ZONED TIME", "LOCAL DATETIME", "ZONED DATETIME", "DURATION"),
                    CypherTypeValueMapper.valueType(value));
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
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N26)
                            .build())
                    .build();
            throw new InvalidArgumentException(
                    gql,
                    "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.");
        }

        if (in instanceof NumberValue inNumber && precisionValue instanceof NumberValue) {
            int precision = asIntExact(
                    precisionValue, () -> "Invalid input for precision value in function 'round()'", "round");
            boolean explicitMode = ((BooleanValue) explicitModeValue).booleanValue();
            if (precision < 0) {
                throw InvalidArgumentException.negRoundPrecision(precision);
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
            throw needsNumbers("round", in);
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
            throw needsNumbers("abs", in);
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
            throw needsNumbers("isNaN", in);
        }
    }

    public static AnyValue toDegrees(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toDegrees(number.doubleValue()));
        } else {
            throw needsNumbers("degrees", in);
        }
    }

    public static AnyValue exp(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.exp(number.doubleValue()));
        } else {
            throw needsNumbers("exp", in);
        }
    }

    public static AnyValue log(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log(number.doubleValue()));
        } else {
            throw needsNumbers("log", in);
        }
    }

    public static AnyValue log10(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log10(number.doubleValue()));
        } else {
            throw needsNumbers("log10", in);
        }
    }

    public static AnyValue toRadians(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toRadians(number.doubleValue()));
        } else {
            throw needsNumbers("radians", in);
        }
    }

    @CalledFromGeneratedCode
    public static ListValue range(AnyValue startValue, AnyValue endValue) {
        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'", "range"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'", "range"),
                1L);
    }

    public static ListValue range(AnyValue startValue, AnyValue endValue, AnyValue stepValue) {
        long step = asLong(stepValue, () -> "Invalid input for step value in function 'range()'", "range");
        if (step == 0L) {
            throw InvalidArgumentException.zeroStepRange();
        }

        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'", "range"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'", "range"),
                step);
    }

    @CalledFromGeneratedCode
    public static AnyValue signum(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return longValue((long) Math.signum(number.doubleValue()));
        } else {
            throw needsNumbers("sign", in);
        }
    }

    public static AnyValue sqrt(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.sqrt(number.doubleValue()));
        } else {
            throw needsNumbers("sqrt", in);
        }
    }

    public static DoubleValue rand() {
        return doubleValue(ThreadLocalRandom.current().nextDouble());
    }

    public static TextValue randomUuid() {
        return stringValue(UUID.randomUUID().toString());
    }

    public static AnyValue generateUUID() {
        return Values.uuidValue(ofEpochMillis(System.currentTimeMillis()));
    }

    // Generate a V7 UUID
    // This is not cryptographically secure.
    static UUID ofEpochMillis(long timestamp) {
        if ((timestamp >> 48) != 0) {
            throw new IllegalArgumentException("Supplied timestamp: " + timestamp + " does not fit within 48 bits");
        }

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long random1 = rng.nextLong();
        long random2 = rng.nextLong();

        long msb = (timestamp << 16) // 48-bit timestamp in upper bits
                | 0x7000L // version 7
                | (random1 & 0x0FFFL); // 12 bits of random data

        long lsb = 0x8000_0000_0000_0000L // variant field at bits 63 & 62 (always "2")
                | (random2 & 0x3FFF_FFFF_FFFF_FFFFL); // 62 bits of random data

        return new UUID(msb, lsb);
    }

    public static AnyValue UUIDFromString(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        }
        if (in instanceof TextValue string) {
            try {
                return Values.uuidValue(string.stringValue());
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(
                        GqlHelper.getGql22N38_22N04(
                                "uuid",
                                string.stringValue(),
                                "input",
                                List.of(
                                        "The UUID string must consist of 32 hexadecimal digits displayed in 5 groups, separated by 4 hyphens.")),
                        "The argument `input` in the `uuid()` function must be a valid uuid string.");
            }
        }
        throw notAString("uuid", in);
    }

    public static AnyValue UUIDFromLongs(AnyValue mostSigBits, AnyValue leastSigBits) {
        if (mostSigBits == NO_VALUE || leastSigBits == NO_VALUE) {
            return NO_VALUE;
        }
        if (mostSigBits instanceof IntegralValue msb && leastSigBits instanceof IntegralValue lsb) {
            return Values.uuidValue(msb.longValue(), lsb.longValue());
        }
        if (!(mostSigBits instanceof IntegralValue)) {
            throw CypherTypeException.functionArgumentWrongType(
                    "uuid() requires integers",
                    "uuid",
                    mostSigBits.prettify(),
                    List.of("INTEGER"),
                    CypherTypeValueMapper.valueType(mostSigBits));
        }
        throw CypherTypeException.functionArgumentWrongType(
                "uuid() requires integers",
                "uuid",
                leastSigBits.prettify(),
                List.of("INTEGER"),
                CypherTypeValueMapper.valueType(leastSigBits));
    }

    public static AnyValue UUIDMostSignificantBits(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        }
        if (in instanceof UUIDValue uuid) {
            return Values.longValue(uuid.getMostSignificantBits());
        }
        throw CypherTypeException.functionArgumentWrongType(
                "uuid.mostSignificantBits() requires a uuid",
                "uuid.mostSignificantBits",
                in.prettify(),
                List.of("UUID"),
                CypherTypeValueMapper.valueType(in));
    }

    public static AnyValue UUIDLeastSignificantBits(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        }
        if (in instanceof UUIDValue uuid) {
            return Values.longValue(uuid.getLeastSignificantBits());
        }
        throw CypherTypeException.functionArgumentWrongType(
                "uuid.leastSignificantBits() requires a uuid",
                "uuid.leastSignificantBits",
                in.prettify(),
                List.of("UUID"),
                CypherTypeValueMapper.valueType(in));
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

    public static Value int8Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.int8Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseInt8Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    public static Value int16Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.int16Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseInt16Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    public static Value int32Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.int32Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseInt32Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    public static Value int64Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.int64Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseInt64Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    public static Value float32Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.float32Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseFloat32Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    public static Value float64Vector(AnyValue in, AnyValue dimension) {
        if (in == NO_VALUE || dimension == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sequence) {
            return assertDimension(VectorUtils.float64Vector(sequence), dimension);
        } else if (in instanceof TextValue textValue) {
            return assertDimension(CypherRuntimeParser.parseFloat64Vector(textValue.stringValue()), dimension);
        } else {
            throw invalidVector(in);
        }
    }

    @CalledFromGeneratedCode
    public static Value vectorSimilarityEuclidean(AnyValue lhs, AnyValue rhs) {
        return vectorSimilarity(VectorSimilarity.EUCLIDEAN, lhs, rhs);
    }

    @CalledFromGeneratedCode
    public static Value vectorSimilarityCosine(AnyValue lhs, AnyValue rhs) {
        return vectorSimilarity(VectorSimilarity.COSINE, lhs, rhs);
    }

    public static AnyValue vectorDistance(AnyValue vector1, AnyValue vector2, AnyValue distanceMetric) {
        if (vector1 == NO_VALUE || vector2 == NO_VALUE) {
            return NO_VALUE;
        }
        if (vector1 instanceof VectorValue v1 && vector2 instanceof VectorValue v2) {
            if (distanceMetric instanceof TextValue textDistanceMetric) {
                if (v1.dimensions() != v2.dimensions()) {
                    throw new InvalidArgumentException(
                            GqlHelper.getGql22N38_22N04(
                                    "vector_distance()",
                                    "`vector1` of dimension " + v1.dimensions() + " and `vector2` of dimension "
                                            + v2.dimensions(),
                                    "vector1 and vector2",
                                    List.of("vector arguments must be the same dimension")),
                            "The argument `vector1` and `vector2` in the `vector_distance()` function must be of the same dimension.");
                }
                return switch (textDistanceMetric.stringValue()) {
                    case "COSINE" -> doubleValue(GQLVectorDistanceFunction.COSINE.distance(v1, v2));
                    case "EUCLIDEAN" -> doubleValue(GQLVectorDistanceFunction.EUCLIDEAN.distance(v1, v2));
                    case "EUCLIDEAN_SQUARED" ->
                        doubleValue(GQLVectorDistanceFunction.EUCLIDEAN_SQUARED.distance(v1, v2));
                    case "MANHATTAN" -> doubleValue(GQLVectorDistanceFunction.MANHATTAN.distance(v1, v2));
                    case "DOT" -> doubleValue(GQLVectorDistanceFunction.DOT.distance(v1, v2));
                    case "HAMMING" -> doubleValue(GQLVectorDistanceFunction.HAMMING.distance(v1, v2));
                    // This is technically a keyword in the parser, so should never fail here.
                    default ->
                        throw InternalException.internalError(
                                CypherFunctions.class.getSimpleName(),
                                "Expected known distance metric, got: " + distanceMetric);
                };
            }
            // This is technically a keyword in the parser, so should never fail here.
            throw InternalException.internalError(
                    CypherFunctions.class.getSimpleName(),
                    "Expected known distance metric, got: " + distanceMetric.prettyPrint());
        }

        if (!(vector1 instanceof VectorValue)) {
            throw notAVector("vector_distance", vector1);
        }
        throw notAVector("vector_distance", vector2);
    }

    public static AnyValue vectorNorm(AnyValue vector, AnyValue distanceMetric) {
        if (vector == NO_VALUE) {
            return NO_VALUE;
        }
        if (vector instanceof VectorValue v) {
            if (distanceMetric instanceof TextValue textDistanceMetric) {
                return switch (textDistanceMetric.stringValue()) {
                    case "EUCLIDEAN" -> doubleValue(GQLVectorNorm.EUCLIDEAN.norm(v));
                    case "MANHATTAN" -> doubleValue(GQLVectorNorm.MANHATTAN.norm(v));
                    // This is technically a keyword in the parser, so should never fail here.
                    default ->
                        throw InternalException.internalError(
                                CypherFunctions.class.getSimpleName(),
                                "Expected known distance metric, got: " + distanceMetric);
                };
            }
            // This is technically a keyword in the parser, so should never fail here.
            throw InternalException.internalError(
                    CypherFunctions.class.getSimpleName(),
                    "Expected known distance metric, got: " + distanceMetric.prettyPrint());
        }

        throw notAVector("vector_norm", vector);
    }

    public static Value vectorSimilarity(VectorSimilarity similarity, AnyValue lhs, AnyValue rhs) {
        final var function = similarity.latestImplementation();
        final var functionName =
                switch (similarity) {
                    case EUCLIDEAN -> VectorSimilarityEuclidean.name();
                    case COSINE -> VectorSimilarityCosine.name();
                };

        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        }

        final var a = toFloatArrayVector(function, functionName, lhs, "a");
        final var b = toFloatArrayVector(function, functionName, rhs, "b");

        if (a.length != b.length) {
            throw InvalidArgumentException.invalidFunctionArgument(
                    functionName,
                    invalidSimilarityFunctionInputErrorMessage(
                            function, "The supplied vectors do not have the same number of dimensions"));
        }

        return doubleValue(function.compare(a, b));
    }

    private static float[] toFloatArrayVector(
            VectorSimilarityFunction function, String functionName, AnyValue arg, String argName) {
        final VectorCandidate candidate = VectorCandidate.maybeFrom(arg);
        if (candidate == null) {
            throw CypherTypeException.functionArgumentWrongType(
                    "Expected argument %s to be a LIST<INTEGER | FLOAT>".formatted(argName),
                    functionName,
                    arg.prettify(),
                    List.of("LIST<INTEGER | FLOAT>"),
                    CypherTypeValueMapper.valueType(arg));
        }

        final float[] floatArray = function.maybeToValidVector(candidate);
        if (floatArray == null) {
            throw InvalidArgumentException.invalidFunctionArgument(
                    functionName,
                    invalidSimilarityFunctionInputErrorMessage(
                            function,
                            "Argument %s is not a valid vector for this similarity function".formatted(argName)));
        }

        return floatArray;
    }

    private static String invalidSimilarityFunctionInputErrorMessage(VectorSimilarityFunction function, String reason) {
        return "Invalid input for 'vector.similarity.%s()': %s."
                .formatted(function.functionName().toLowerCase(), reason);
    }

    public static AnyValue startNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.startNode();
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return startNode(rel, access, cursor);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'startNode()': Expected %s to be a RelationshipValue",
                            anyValue),
                    "startNode",
                    anyValue.prettify(),
                    List.of("RELATIONSHIP"),
                    CypherTypeValueMapper.valueType(anyValue));
        }
    }

    public static VirtualNodeValue startNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        if (relationship instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.startNode();
        }
        return VirtualValues.node(relationship.startNodeId(consumer(access, cursor)));
    }

    public static AnyValue endNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.endNode();
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return endNode(rel, access, cursor);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'endNode()': Expected %s to be a RelationshipValue", anyValue),
                    "endNode",
                    anyValue.prettify(),
                    List.of("RELATIONSHIP"),
                    CypherTypeValueMapper.valueType(anyValue));
        }
    }

    public static VirtualNodeValue endNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        if (relationship instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.startNode();
        }
        return VirtualValues.node(relationship.endNodeId(consumer(access, cursor)));
    }

    @CalledFromGeneratedCode
    public static VirtualNodeValue otherNode(
            AnyValue anyValue, DbAccess access, VirtualNodeValue node, RelationshipScanCursor cursor) {
        // This is not a function exposed to the user
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (anyValue instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.otherNode(node);
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return otherNode(rel, access, node, cursor);
        } else {
            throw CypherTypeException.expectedRelValue(
                    anyValue.toString(), anyValue.prettyPrint(), CypherTypeValueMapper.valueType(anyValue));
        }
    }

    public static VirtualNodeValue otherNode(
            VirtualRelationshipValue relationship,
            DbAccess access,
            VirtualNodeValue node,
            RelationshipScanCursor cursor) {
        if (relationship instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.otherNode(node);
        }
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
        } else if (container instanceof NodeValue node && node.id() < 0) {
            return node.properties().get(key);
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), dbAccess.propertyKey(key), nodeCursor, propertyCursor, true);
        } else if (container instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.properties().get(key);
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
            throw CypherTypeException.expectedMap(
                    container.toString(), container.prettyPrint(), CypherTypeValueMapper.valueType(container));
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
        if (container instanceof NodeValue node && node.id() < 0) {
            AnyValue[] values = new AnyValue[keys.length];
            for (int i = 0; i < keys.length; i++) {
                MapValue nodeProps = node.properties();
                if (nodeProps.containsKey(keys[i])) {
                    values[i] = nodeProps.get(keys[i]);
                } else {
                    values[i] = NO_VALUE;
                }
            }
            return values;
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperties(node.id(), propertyKeys(keys, dbAccess), nodeCursor, propertyCursor);
        } else if (container instanceof RelationshipValue rel && rel.id() < 0) {
            AnyValue[] values = new AnyValue[keys.length];
            for (int i = 0; i < keys.length; i++) {
                MapValue nodeProps = rel.properties();
                if (nodeProps.containsKey(keys[i])) {
                    values[i] = nodeProps.get(keys[i]);
                } else {
                    values[i] = NO_VALUE;
                }
            }
            return values;
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
        } else if (container instanceof NodeValue node && node.id() < 0 && index instanceof TextValue) {
            return node.properties().get(propertyKeyName(index));
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor, true);
        } else if (container instanceof RelationshipValue rel && rel.id() < 0 && index instanceof TextValue) {
            return rel.properties().get(propertyKeyName(index));
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperty(
                    rel, propertyKeyId(dbAccess, index), relationshipScanCursor, propertyCursor, true);
        }
        if (container instanceof MapValue map) {
            return mapAccess(map, index);
        } else if (container instanceof SequenceValue seq) {
            return listAccess(seq, index);
        } else {
            throw CypherTypeException.notCollectionOrMap(
                    container.toString(), container.prettyPrint(), CypherTypeValueMapper.valueType(container), index);
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
        } else if (container instanceof NodeValue node && node.id() < 0 && index instanceof TextValue) {
            return booleanValue(node.properties().containsKey(propertyKeyName(index)));
        } else if (container instanceof VirtualNodeValue node) {
            return booleanValue(
                    dbAccess.nodeHasProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor));
        } else if (container instanceof RelationshipValue rel && rel.id() < 0 && index instanceof TextValue) {
            return booleanValue(rel.properties().containsKey(propertyKeyName(index)));
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
            throw CypherTypeException.notMap(
                    container.toString(), container.prettyPrint(), CypherTypeValueMapper.valueType(container), index);
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue head(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof SequenceValue sequence) {
            return sequence.head();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'head()': Expected %s to be a list", container),
                    "head",
                    container.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(container));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue tail(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof ListValue list) {
            return list.tail();
        } else if (container instanceof ArrayValue array) {
            return VirtualValues.fromArray(array).tail();
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
            return sequence.last();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'last()': Expected %s to be a list", container),
                    "last",
                    container.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(container));
        }
    }

    public static AnyValue left(AnyValue in, AnyValue endPos) {
        if (in == NO_VALUE || endPos == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            final int len =
                    asIntExact(endPos, () -> "Invalid input for length value in function 'left()'", "left", true, 0);
            return text.substring(0, len);
        } else {
            throw notAString("left", in);
        }
    }

    public static AnyValue ltrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).ltrim();
        } else {
            throw notAString("ltrim", trimSource);
        }
    }

    public static AnyValue ltrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.ltrim(trimCharacterStringText);
            } else {
                throw notAString("ltrim", trimCharacterString);
            }
        } else {
            throw notAString("ltrim", trimSource);
        }
    }

    public static AnyValue collSort(AnyValue list) {
        if (list == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue seq) {
            List<AnyValue> newList = new ArrayList<>();
            if (seq.iterationPreference() == SequenceValue.IterationPreference.RANDOM_ACCESS) {
                for (int i = 0; i < seq.intSize(); i++) {
                    newList.add(seq.value(i));
                }
            } else {
                for (AnyValue anyValue : seq) {
                    newList.add(anyValue);
                }
            }
            newList.sort(AnyValues.COMPARATOR);

            return fromList(newList);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.sort()': Expected %s to be a list", list),
                    "coll.sort",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collIndexOf(AnyValue list, AnyValue value) {
        if (list == NO_VALUE || value == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            return intValue(sequence.indexOf(value));
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.indexOf()': Expected %s to be a list", list),
                    "coll.indexOf",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collMax(AnyValue list) {
        if (list == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            if (sequence.isEmpty()) {
                return NO_VALUE;
            }
            AnyValue maxValue = sequence.head();
            if (sequence.iterationPreference() == SequenceValue.IterationPreference.RANDOM_ACCESS) {
                // use a for loop; skip head as we already assume that is max
                for (int i = 1; i < sequence.intSize(); i++) {
                    var anyValue = sequence.value(i);
                    if (AnyValues.TERNARY_COMPARATOR.compare(anyValue, maxValue) > 0) {
                        maxValue = anyValue;
                    }
                }
            } else {
                // use the iterator
                for (AnyValue anyValue : sequence) {
                    if (AnyValues.TERNARY_COMPARATOR.compare(anyValue, maxValue) > 0) {
                        maxValue = anyValue;
                    }
                }
            }

            return maxValue;
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.max()': Expected %s to be a list", list),
                    "coll.max",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collMin(AnyValue list) {
        if (list == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            AnyValue minValue = NO_VALUE;
            if (sequence.iterationPreference() == SequenceValue.IterationPreference.RANDOM_ACCESS) {
                // use a for loop
                for (int i = 0; i < sequence.intSize(); i++) {
                    var anyValue = sequence.value(i);
                    if (AnyValues.TERNARY_COMPARATOR.compare(anyValue, minValue) < 0) {
                        minValue = anyValue;
                    }
                }
            } else {
                // use the iterator
                for (AnyValue anyValue : sequence) {
                    if (AnyValues.TERNARY_COMPARATOR.compare(anyValue, minValue) < 0) {
                        minValue = anyValue;
                    }
                }
            }
            return minValue;
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.min()': Expected %s to be a list", list),
                    "coll.min",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collFlatten(AnyValue list) {
        if (list == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            return sequence.flatten(1);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.flatten()': Expected %s to be a list", list),
                    "coll.flatten",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collFlatten(AnyValue list, AnyValue depth) {
        if (list == NO_VALUE || depth == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            int maxDepth = asIntExact(
                    depth, () -> "Invalid input for depth value in function 'coll.flatten()'", "coll.flatten", false);
            if (maxDepth < 0) {
                throw InvalidArgumentException.argumentOutOfRange(
                        "coll.flatten", "depth", 0, Integer.MAX_VALUE, maxDepth);
            }

            return sequence.flatten(maxDepth);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.flatten()': Expected %s to be a list", list),
                    "coll.flatten",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collInsert(AnyValue list, AnyValue index, AnyValue value) {
        if (list == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            int givenIndex = asIntExact(
                    index, () -> "Invalid input for index value in function 'coll.insert()'", "coll.insert", false);
            if (givenIndex < 0 || givenIndex > sequence.intSize()) {
                throw InvalidArgumentException.argumentOutOfRange(
                        "coll.insert", "index", 0, sequence.intSize(), givenIndex);
            }

            return sequence.insertAt(givenIndex, value);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.insert()': Expected %s to be a list", list),
                    "coll.insert",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collRemove(AnyValue list, AnyValue index) {
        if (list == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            if (sequence.intSize() == 0) {
                throw new InvalidArgumentException(
                        GqlHelper.getGql22N38_22N04(
                                "coll.remove()", "[]", "list", List.of("argument list must not be empty")),
                        "The argument `list` in the `coll.remove()` function must not be empty.");
            }
            int givenIndex = asIntExact(
                    index, () -> "Invalid input for index value in function 'coll.remove()'", "coll.remove", false);
            if (givenIndex < 0 || givenIndex >= sequence.intSize()) {
                throw InvalidArgumentException.argumentOutOfRange(
                        "coll.remove", "index", 0, sequence.intSize() - 1, givenIndex);
            }

            return sequence.remove(givenIndex);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.remove()': Expected %s to be a list", list),
                    "coll.remove",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue collDistinct(AnyValue list) {
        if (list == NO_VALUE) {
            return NO_VALUE;
        } else if (list instanceof SequenceValue sequence) {
            return sequence.asListValue().distinct();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'coll.distinct()': Expected %s to be a list", list),
                    "coll.distinct",
                    list.prettyPrint(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue rtrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).rtrim();
        } else {
            throw notAString("rtrim", trimSource);
        }
    }

    public static AnyValue rtrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.rtrim(trimCharacterStringText);
            } else {
                throw notAString("rtrim", trimCharacterString);
            }
        } else {
            throw notAString("rtrim", trimSource);
        }
    }

    public static AnyValue btrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).trim();
        } else {
            throw notAString("btrim", trimSource);
        }
    }

    public static AnyValue btrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.trim(trimCharacterStringText);
            } else {
                throw notAString("btrim", trimCharacterString);
            }
        } else {
            throw notAString("btrim", trimSource);
        }
    }

    public static AnyValue trim(AnyValue trimSpecification, AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        }

        if (!(trimSource instanceof TextValue)) {
            throw notAString("trim", trimSource);
        }

        if (trimSpecification instanceof TextValue trimSpec) {
            return switch (trimSpec.stringValue()) {
                case "LEADING" -> ltrim(trimSource);
                case "TRAILING" -> rtrim(trimSource);
                default -> btrim(trimSource);
            };
        } else {
            throw notAString("trim", trimSpecification);
        }
    }

    public static AnyValue trim(AnyValue trimSpecification, AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        }

        if (!(trimSource instanceof TextValue)) {
            throw notAString("trim", trimSource);
        }

        if (trimSpecification instanceof TextValue trimSpec) {
            if (trimCharacterString instanceof TextValue trimCharString) {
                if (trimCharString.length() != 1) {
                    throw new InvalidArgumentException(
                            GqlHelper.getGql22N38_22N04(
                                    "trim()",
                                    trimCharString.prettyPrint(),
                                    "trimCharacterString",
                                    List.of("argument to be of length 1")),
                            "The argument `trimCharacterString` in the `trim()` function must be of length 1.");
                }
            }
            return switch (trimSpec.stringValue()) {
                case "LEADING" -> ltrim(trimSource, trimCharacterString);
                case "TRAILING" -> rtrim(trimSource, trimCharacterString);
                default -> btrim(trimSource, trimCharacterString);
            };
        } else {
            throw notAString("trim", trimSpecification);
        }
    }

    public static AnyValue replace(AnyValue original, AnyValue search, AnyValue replaceWith) {
        if (original == NO_VALUE || search == NO_VALUE || replaceWith == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue textValue) {
            return textValue.replace(asString(search), asString(replaceWith));
        } else {
            throw notAString("replace", original);
        }
    }

    public static AnyValue replace(AnyValue original, AnyValue search, AnyValue replaceWith, AnyValue limit) {
        if (original == NO_VALUE || search == NO_VALUE || replaceWith == NO_VALUE || limit == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue textValue) {
            int intLimit =
                    asIntExact(limit, () -> "Invalid input for limit value in function 'replace()'", "replace", false);
            if (intLimit < 0) {
                throw InvalidArgumentException.argumentOutOfRange("replace", "limit", 0, Long.MAX_VALUE, intLimit);
            }
            return textValue.replaceWithLimit(asString(search), asString(replaceWith), intLimit);
        } else {
            throw notAString("replace", original);
        }
    }

    public static AnyValue stringIndexOf(AnyValue input, AnyValue value) {
        if (input == NO_VALUE || value == NO_VALUE) {
            return NO_VALUE;
        } else if (input instanceof TextValue inputText && value instanceof TextValue valueText) {
            int utf16Idx = inputText.stringValue().indexOf(valueText.stringValue());
            int cpIdx = utf16Idx < 0 ? -1 : inputText.stringValue().codePointCount(0, utf16Idx);
            return intValue(cpIdx);
        } else if (!(input instanceof TextValue)) {
            throw notAString("string.indexOf", input);
        } else {
            throw notAString("string.indexOf", value);
        }
    }

    public static AnyValue stringJoin(AnyValue list, AnyValue delimiter) {
        if (list == NO_VALUE || delimiter == NO_VALUE) {
            return NO_VALUE;
        } else if (!(delimiter instanceof TextValue delimText)) {
            throw notAString("string.join", delimiter);
        } else if (list instanceof SequenceValue sequence) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (AnyValue item : sequence) {
                if (item == NO_VALUE) {
                    continue;
                }
                if (!(item instanceof TextValue)) {
                    throw notAString("string.join", item);
                }
                if (!first) {
                    sb.append(delimText.stringValue());
                }
                sb.append(((TextValue) item).stringValue());
                first = false;
            }
            return stringValue(sb.toString());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'string.join()': Expected %s to be a list", list),
                    "string.join",
                    list.prettyPrint(),
                    List.of("LIST<STRING>"),
                    CypherTypeValueMapper.valueType(list));
        }
    }

    public static AnyValue stringRegexReplaceWithPattern(AnyValue input, Pattern pattern, AnyValue replacement) {
        if (input == NO_VALUE || replacement == NO_VALUE) {
            return NO_VALUE;
        } else if (!(input instanceof TextValue inputText)) {
            throw notAString("string.regexReplace", input);
        } else if (!(replacement instanceof TextValue replacementText)) {
            throw notAString("string.regexReplace", replacement);
        } else {
            return stringValue(pattern.matcher(inputText.stringValue()).replaceAll(replacementText.stringValue()));
        }
    }

    public static AnyValue stringRegexReplace(AnyValue input, AnyValue regex, AnyValue replacement) {
        if (input == NO_VALUE || regex == NO_VALUE || replacement == NO_VALUE) {
            return NO_VALUE;
        } else if (!(input instanceof TextValue inputText)) {
            throw notAString("string.regexReplace", input);
        } else if (!(regex instanceof TextValue regexText)) {
            throw notAString("string.regexReplace", regex);
        } else if (!(replacement instanceof TextValue replacementText)) {
            throw notAString("string.regexReplace", replacement);
        } else {
            try {
                return stringValue(Pattern.compile(regexText.stringValue())
                        .matcher(inputText.stringValue())
                        .replaceAll(replacementText.stringValue()));
            } catch (PatternSyntaxException e) {
                throw InvalidSemanticsException.invalidRegex(e.getMessage(), regexText.stringValue());
            }
        }
    }

    public static AnyValue reverse(AnyValue original) {
        if (original == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue text) {
            return text.reverse();
        } else if (original instanceof SequenceValue list) {
            return list.reverse();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'reverse()': "
                            + "Expected a string or a list; consider converting the value to a string with toString() or creating a list.",
                    "reverse",
                    original.prettify(),
                    List.of("STRING", "LIST<ANY>"),
                    CypherTypeValueMapper.valueType(original));
        }
    }

    public static AnyValue right(AnyValue original, AnyValue length) {
        if (original == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            final long len = asLong(length, () -> "Invalid input for length value in function 'right()'", "right");
            if (len < 0) {
                throw new IndexOutOfBoundsException("negative length");
            }
            final long startVal = asText.length() - len;
            return asText.substring((int) Math.max(0, startVal));
        } else {
            throw notAString("right", original);
        }
    }

    public static AnyValue concatenate(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE && rhs == NO_VALUE) {
            return NO_VALUE;
        }
        // List Concatenation - Can only concatenate lists when both sides are lists
        // arrays are same as lists when it comes to concatenation
        if (lhs instanceof ArrayValue array) {
            lhs = VirtualValues.fromArray(array);
        }
        if (rhs instanceof ArrayValue array) {
            rhs = VirtualValues.fromArray(array);
        }

        // Null values only return null if the other side is either null (checked already) or a valid concatenation
        // type.
        if (lhs == NO_VALUE) {
            if (rhs instanceof ListValue || rhs instanceof TextValue) {
                return NO_VALUE;
            }
        }

        if (rhs == NO_VALUE) {
            if (lhs instanceof ListValue || lhs instanceof TextValue) {
                return NO_VALUE;
            }
        }

        boolean lhsIsListValue = lhs instanceof ListValue;
        if (lhsIsListValue && rhs instanceof ListValue) {
            return ((ListValue) lhs).appendAll((ListValue) rhs);
        }

        // String Concatenation - Can only concatenate strings when both sides are strings
        if (lhs instanceof TextValue && rhs instanceof TextValue) {
            return ((TextValue) lhs).plus((TextValue) rhs);
        }

        throw CypherTypeException.concatenationTypeMismatch(
                rhs.prettyPrint(),
                lhs.getTypeName(),
                rhs.getTypeName(),
                CypherTypeValueMapper.valueType(rhs),
                CypherTypeValueMapper.valueType(lhs));
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
            throw InvalidArgumentException.unknownNormalForm(String.valueOf(normalForm));
        }

        String normalized = Normalizer.normalize(asTextValue(input).stringValue(), form);
        return Values.stringValue(normalized);
    }

    public static AnyValue split(AnyValue original, AnyValue separator) {
        if (original == NO_VALUE || separator == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            if (asText.isEmpty()) {
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

            return asText.substring(asIntExact(
                    start, () -> "Invalid input for start value in function 'substring()'", "substring", true, 0));
        } else {
            throw notAString("substring", original);
        }
    }

    public static AnyValue substring(AnyValue original, AnyValue start, AnyValue length) {
        if (original == NO_VALUE || start == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {

            return asText.substring(
                    asIntExact(
                            start,
                            () -> "Invalid input for start value in function 'substring()'",
                            "substring",
                            true,
                            0),
                    asIntExact(
                            length,
                            () -> "Invalid input for length value in function 'substring()'",
                            "substring",
                            true,
                            0));
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
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'id()': Expected %s to be a node or relationship, but it was `%s`",
                            item, item.getTypeName()),
                    "id",
                    item.prettify(),
                    List.of("NODE", "RELATIONSHIP"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue elementId(AnyValue entity, ElementIdMapper idMapper) {
        if (entity == NO_VALUE) {
            return NO_VALUE;
        } else if (entity instanceof NodeValue node) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(node.elementId());
        } else if (entity instanceof VirtualNodeValue node && node.id() < 0) {
            // Don't format element ids for nodes with negative ids, such as db schema visualization
            return stringValue(String.valueOf(node.id()));
        } else if (entity instanceof VirtualNodeValue node) {
            return stringValue(idMapper.nodeElementId(node.id()));
        } else if (entity instanceof RelationshipValue relationship) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(relationship.elementId());
        } else if (entity instanceof VirtualRelationshipValue relationship && relationship.id() < 0) {
            // Don't format element ids for relationships with negative ids, such as db schema visualization
            return stringValue(String.valueOf(relationship.id()));
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return stringValue(idMapper.relationshipElementId(relationship.id()));
        }

        throw CypherTypeException.functionArgumentWrongType(
                String.format(
                        "Invalid input for function 'elementId()': Expected %s to be a node or relationship, but it was `%s`",
                        entity, entity.getTypeName()),
                "elementId",
                entity.prettify(),
                List.of("NODE", "RELATIONSHIP"),
                CypherTypeValueMapper.valueType(entity));
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
            var builder = ListValueBuilder.newListBuilder(elementIds.intSize());
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
            var builder = ListValueBuilder.newListBuilder(elementIds.intSize());
            for (var elementId : elementIds) {
                AnyValue value = elementIdToRelationshipId(elementId, idMapper);
                builder.add(value);
            }
            return builder.build();
        }
        return NO_VALUE;
    }

    public static TextValue nodeElementId(long id, ElementIdMapper idMapper) {
        assert id > LongReference.NULL;
        return Values.stringValue(idMapper.nodeElementId(id));
    }

    public static TextValue relationshipElementId(long id, ElementIdMapper idMapper) {
        assert id > LongReference.NULL;
        return Values.stringValue(idMapper.relationshipElementId(id));
    }

    public static AnyValue labels(AnyValue item, DbAccess access, NodeCursor nodeCursor) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof NodeValue node && node.id() < 0) {
            // Labels for entities with negative id, such as db schema visualization, are already populated since
            // the entity isn't a node in storage
            var builder = ListValueBuilder.newListBuilder(node.labels().intSize());
            node.labels().forEach(builder::add);
            return builder.build();
        } else if (item instanceof VirtualNodeValue node) {
            return access.getLabelsForNode(node.id(), nodeCursor);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'labels()': Expected a Node, got: " + item,
                    "labels",
                    item.prettify(),
                    List.of("NODE"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabel(AnyValue entity, int labelToken, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isLabelSetOnNode(labelToken, node.id(), nodeCursor);
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabels(AnyValue entity, int[] labelTokens, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.areLabelsSetOnNode(labelTokens, node.id(), nodeCursor);
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    private static boolean hasLabel(
            VirtualNodeValue node, TextValue textLabel, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        var validName = checkValidTokenName(textLabel.stringValue(), TokenType.LABEL);
        var tokenId = queryContext.nodeLabel(validName);
        if (tokenId == TokenConstants.NO_TOKEN) {
            return false;
        }

        return queryContext.isLabelSetOnNode(tokenId, node.id(), nodeCursor);
    }

    public static String evaluateSingleDynamicRelType(AnyValue value) {
        if (value == NO_VALUE) {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected relationship type to be a string or list of strings.", "NULL", "NULL");
        } else if (value instanceof TextValue textValue) {
            return textValue.stringValue();
        } else if (value instanceof SequenceValue sequenceValue) {
            if (sequenceValue.actualSize() != 1L) {
                throw new IllegalArgumentException(String.format(
                        "Exactly one relationship type must be specified, but %d were found.",
                        sequenceValue.actualSize()));
            }

            var t = sequenceValue.value(0);
            if (t instanceof TextValue textValue) {
                return textValue.stringValue();
            } else {
                throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                        "Expected relationship type to be a string or list of strings.",
                        t.prettyPrint(),
                        CypherTypeValueMapper.valueType(t));
            }
        } else {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected relationship type to be a string or list of strings.",
                    value.prettyPrint(),
                    CypherTypeValueMapper.valueType(value));
        }
    }

    public static int getOrCreateDynamicRelType(AnyValue value, QueryContext queryContext) {
        return queryContext.getOrCreateRelTypeId(evaluateSingleDynamicRelType(value));
    }

    public static int getOrCreateDynamicRelType(AnyValue value, TokenWrite token) throws KernelException {
        return token.relationshipTypeGetOrCreateForName(evaluateSingleDynamicRelType(value));
    }

    @CalledFromGeneratedCode
    public static boolean hasDynamicLabelsOrTypes(
            AnyValue entity,
            AnyValue[] labelOrTypes,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            QueryContext queryContext,
            RuntimeNotifier notifier)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return hasDynamicLabels(node, labelOrTypes, nodeCursor, queryContext);
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return hasDynamicType(relationship, labelOrTypes, relCursor, queryContext, notifier);
        } else {
            throw CypherTypeException.invalidTypeForLabelExpression(
                    entity.toString(),
                    entity.prettify(),
                    entity.getTypeName(),
                    CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasDynamicLabels(
            AnyValue entity, AnyValue[] labelNames, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        boolean allMatch = true;
        if (entity instanceof VirtualNodeValue node) {
            for (var labelName : labelNames) {
                if (labelName instanceof TextValue textLabel) {
                    if (!hasLabel(node, textLabel, nodeCursor, queryContext)) {
                        allMatch = false;
                    }
                } else if (labelName instanceof SequenceValue labelSequence) {
                    for (var l : labelSequence) {
                        if (l instanceof TextValue textLabel) {
                            if (!hasLabel(node, textLabel, nodeCursor, queryContext)) {
                                allMatch = false;
                            }
                        } else {
                            throw CypherTypeException.expectedStringNotNull(
                                    "Expected node label to be a string or list of strings.",
                                    l == NO_VALUE ? "NULL" : l.prettyPrint(),
                                    CypherTypeValueMapper.valueType(l));
                        }
                    }
                } else {
                    throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                            "Expected node label to be a string or list of strings.",
                            labelName == NO_VALUE ? "NULL" : labelName.prettyPrint(),
                            CypherTypeValueMapper.valueType(labelName));
                }
            }
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
        return allMatch;
    }

    @CalledFromGeneratedCode
    public static String[] getDynamicLabels(AnyValue labelName) throws IllegalTokenNameException {
        if (labelName instanceof TextValue textLabel) {
            return new String[] {checkValidTokenName(textLabel.stringValue(), TokenType.LABEL)};
        } else if (labelName instanceof SequenceValue labelSequence) {
            var list = new String[labelSequence.intSize()];
            int i = 0;
            for (var l : labelSequence) {
                if (l instanceof TextValue textLabel) {
                    list[i++] = checkValidTokenName(textLabel.stringValue(), TokenType.LABEL);
                } else {
                    throw CypherTypeException.expectedStringNotNull(
                            "Expected node label to be a string or list of strings.",
                            l == NO_VALUE ? "NULL" : l.prettyPrint(),
                            CypherTypeValueMapper.valueType(l));
                }
            }
            return list;
        } else {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected node label to be a string or list of strings.",
                    labelName == NO_VALUE ? "NULL" : labelName.prettyPrint(),
                    CypherTypeValueMapper.valueType(labelName));
        }
    }

    public sealed interface GetSingleDynamicTypeResult {
        record SingleDynamicType(String value) implements GetSingleDynamicTypeResult {
            public String getValue() {
                return value;
            }
        }

        record ConflictingDynamicTypes() implements GetSingleDynamicTypeResult {}

        record EmptyDynamicTypeList() implements GetSingleDynamicTypeResult {}
    }

    @CalledFromGeneratedCode
    public static GetSingleDynamicTypeResult getSingleDynamicType(AnyValue typeName, RuntimeNotifier notifier)
            throws IllegalTokenNameException {
        if (typeName instanceof TextValue textType) {
            return new SingleDynamicType(checkValidTokenName(textType.stringValue(), TokenType.RELATIONSHIP_TYPE));
        } else if (typeName instanceof SequenceValue typeSequence) {
            ArrayList<String> conflicts = null;
            String singleValue = null;
            for (var l : typeSequence) {
                if (l instanceof TextValue textType) {
                    String validValue = checkValidTokenName(textType.stringValue(), TokenType.RELATIONSHIP_TYPE);
                    if (singleValue == null) {
                        singleValue = validValue;
                    } else if (!singleValue.equals(validValue)) {
                        if (conflicts == null) {
                            conflicts = new ArrayList<>();
                            conflicts.add(singleValue);
                        }
                        conflicts.add(validValue);
                    }
                } else {
                    throw CypherTypeException.expectedStringNotNull(
                            "Expected relationship type to be a string or list of strings.",
                            l.prettyPrint(),
                            CypherTypeValueMapper.valueType(l));
                }
            }

            if (singleValue == null) {
                return new EmptyDynamicTypeList();
            } else if (conflicts == null) {
                return new SingleDynamicType(singleValue);
            } else {
                notifier.newRuntimeNotification(new RuntimeUnsatisfiableRelationshipTypeExpression(conflicts));
                return new ConflictingDynamicTypes();
            }
        } else {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected relationship type to be a string or list of strings.",
                    typeName == NO_VALUE ? "NULL" : typeName.prettyPrint(),
                    CypherTypeValueMapper.valueType(typeName));
        }
    }

    @CalledFromGeneratedCode
    public static String[] getDynamicTypes(AnyValue labelName) throws IllegalTokenNameException {
        if (labelName instanceof TextValue textType) {
            return new String[] {checkValidTokenName(textType.stringValue(), TokenType.RELATIONSHIP_TYPE)};
        } else if (labelName instanceof SequenceValue labelSequence) {
            var list = new String[labelSequence.intSize()];
            int i = 0;
            for (var l : labelSequence) {
                if (l instanceof TextValue textType) {
                    list[i++] = checkValidTokenName(textType.stringValue(), TokenType.RELATIONSHIP_TYPE);
                } else {
                    throw CypherTypeException.expectedStringNotNull(
                            "Expected relationship type to be a string or list of strings.",
                            l.prettyPrint(),
                            CypherTypeValueMapper.valueType(l));
                }
            }
            return list;
        } else {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected relationship type to be a string or list of strings.",
                    labelName == NO_VALUE ? "NULL" : labelName.prettyPrint(),
                    CypherTypeValueMapper.valueType(labelName));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasALabel(AnyValue entity, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue virtualNodeValue) {
            return access.isALabelSetOnNode(virtualNodeValue.id(), nodeCursor);
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
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
            throw CypherTypeException.invalidTypeForLabelExpression(
                    entity.toString(),
                    entity.prettify(),
                    entity.getTypeName(),
                    CypherTypeValueMapper.valueType(entity));
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
            throw CypherTypeException.invalidTypeForLabelExpression(
                    entity.toString(),
                    entity.prettify(),
                    entity.getTypeName(),
                    CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyLabel(AnyValue entity, int[] labels, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isAnyLabelSetOnNode(labels, node.id(), nodeCursor);
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyDynamicLabelsOrTypes(
            AnyValue entity,
            AnyValue[] labelsOrTypes,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return hasAnyDynamicLabel(node, labelsOrTypes, nodeCursor, queryContext);
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return hasAnyDynamicType(relationship, labelsOrTypes, relCursor, queryContext);
        } else {
            throw CypherTypeException.invalidTypeForLabelExpression(
                    entity.toString(),
                    entity.prettify(),
                    entity.getTypeName(),
                    CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyDynamicLabel(
            AnyValue entity, AnyValue[] labels, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        boolean anyMatch = false;
        if (entity instanceof VirtualNodeValue node) {
            for (var labelName : labels) {
                if (labelName instanceof TextValue textLabel) {
                    if (hasLabel(node, textLabel, nodeCursor, queryContext)) {
                        anyMatch = true;
                    }
                } else if (labelName instanceof SequenceValue labelSequence) {
                    for (var l : labelSequence) {
                        if (l instanceof TextValue textLabel) {
                            if (hasLabel(node, textLabel, nodeCursor, queryContext)) {
                                anyMatch = true;
                            }
                        } else {
                            throw CypherTypeException.expectedStringNotNull(
                                    "Expected node label to be a string or list of strings.",
                                    l == NO_VALUE ? "NULL" : l.prettyPrint(),
                                    CypherTypeValueMapper.valueType(l));
                        }
                    }
                } else {
                    throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                            "Expected node label to be a string or list of strings.",
                            labelName == NO_VALUE ? "NULL" : labelName.prettyPrint(),
                            CypherTypeValueMapper.valueType(labelName));
                }
            }
        } else {
            throw CypherTypeException.expectedNode(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }

        return anyMatch;
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
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'type()': Expected a Relationship, got: " + item,
                    "type",
                    item.prettify(),
                    List.of("RELATIONSHIP"),
                    CypherTypeValueMapper.valueType(item));
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
            throw CypherTypeException.expectedRel(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasTypes(
            AnyValue entity, int[] typeTokens, DbAccess access, RelationshipScanCursor relCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            return access.areTypesSetOnRelationship(typeTokens, relationship, relCursor);
        } else {
            throw CypherTypeException.expectedRel(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    private static boolean hasType(
            VirtualRelationshipValue relationship,
            TextValue textValue,
            RelationshipScanCursor relCursor,
            DbAccess queryContext)
            throws IllegalTokenNameException {
        var validName = checkValidTokenName(textValue.stringValue(), TokenType.RELATIONSHIP_TYPE);
        var tokenId = queryContext.relationshipType(validName);
        return queryContext.isTypeSetOnRelationship(tokenId, relationship.id(), relCursor);
    }

    @CalledFromGeneratedCode
    public static boolean hasDynamicType(
            AnyValue entity,
            AnyValue[] dynamicTypes,
            RelationshipScanCursor relCursor,
            DbAccess queryContext,
            RuntimeNotifier notifier)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        TextValue singleValue = null;
        ArrayList<String> conflictingTypes = null;
        if (entity instanceof VirtualRelationshipValue relationship) {
            for (var value : dynamicTypes) {
                if (value instanceof TextValue textValue) {
                    checkValidTokenName(textValue.stringValue(), TokenType.RELATIONSHIP_TYPE);
                    if (conflictingTypes != null) {
                        conflictingTypes.add(textValue.stringValue());
                    } else if (singleValue == null) {
                        singleValue = textValue;
                    } else if (!singleValue.equals(textValue)) {
                        conflictingTypes = new ArrayList<>();
                        conflictingTypes.add(singleValue.stringValue());
                        conflictingTypes.add(textValue.stringValue());
                    }
                } else if (value instanceof SequenceValue sequenceValue) {
                    for (var t : sequenceValue) {
                        if (t instanceof TextValue textValue) {
                            checkValidTokenName(textValue.stringValue(), TokenType.RELATIONSHIP_TYPE);
                            if (conflictingTypes != null) {
                                conflictingTypes.add(textValue.stringValue());
                            } else if (singleValue == null) {
                                singleValue = textValue;
                            } else if (!singleValue.equals(textValue)) {
                                conflictingTypes = new ArrayList<>();
                                conflictingTypes.add(singleValue.stringValue());
                                conflictingTypes.add(textValue.stringValue());
                            }
                        } else {
                            throw CypherTypeException.expectedStringNotNull(
                                    "Expected relationship type to be a string or list of strings.",
                                    t == NO_VALUE ? "NULL" : t.prettyPrint(),
                                    CypherTypeValueMapper.valueType(t));
                        }
                    }
                } else {
                    throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                            "Expected relationship type to be a string or list of strings.",
                            value == NO_VALUE ? "NULL" : value.prettyPrint(),
                            CypherTypeValueMapper.valueType(value));
                }
            }

            if (singleValue == null) {
                // weird but if we have no value by this point then the list was empty and all(for n in []) == true
                return true;
            }

            if (conflictingTypes != null) {
                // detected more than one relationship type; the conjunction can never be satisfied
                notifier.newRuntimeNotification(new RuntimeUnsatisfiableRelationshipTypeExpression(conflictingTypes));
                return false;
            }

            return hasType(relationship, singleValue, relCursor, queryContext);
        } else {
            throw CypherTypeException.expectedRel(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyDynamicType(
            AnyValue entity, AnyValue[] dynamicTypes, RelationshipScanCursor relCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            for (var typ : dynamicTypes) {
                if (typ instanceof TextValue textValue) {
                    if (hasType(relationship, textValue, relCursor, queryContext)) {
                        return true;
                    }
                } else if (typ instanceof SequenceValue typeSeq) {
                    for (var t : typeSeq) {
                        if (t instanceof TextValue textValue) {
                            if (hasType(relationship, textValue, relCursor, queryContext)) {
                                return true;
                            }
                        } else {
                            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                                    "Expected relationship type to be a string or list of strings.",
                                    t == NO_VALUE ? "NULL" : t.prettyPrint(),
                                    CypherTypeValueMapper.valueType(t));
                        }
                    }
                } else {
                    throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                            "Expected relationship type to be a string or list of strings.",
                            typ == NO_VALUE ? "NULL" : typ.prettyPrint(),
                            CypherTypeValueMapper.valueType(typ));
                }
            }
        } else {
            throw CypherTypeException.expectedRel(
                    entity.toString(), entity.prettyPrint(), CypherTypeValueMapper.valueType(entity));
        }

        return false;
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
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'nodes()': Expected %s to be a path", in),
                    "nodes",
                    in.prettify(),
                    List.of("PATH"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static AnyValue relationships(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualPathValue path) {
            return path.relationshipsAsList();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'relationships()': Expected %s to be a path", in),
                    "relationships",
                    in.prettify(),
                    List.of("PATH"),
                    CypherTypeValueMapper.valueType(in));
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
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'point()': Expected a map but got %s", in),
                    "point",
                    in.prettify(),
                    List.of("MAP"),
                    CypherTypeValueMapper.valueType(in));
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
        } else if (in instanceof NodeValue node && node.id() < 0) {
            return node.properties().keys();
        } else if (in instanceof VirtualNodeValue node) {
            return extractKeys(access, access.nodePropertyIds(node.id(), nodeCursor, propertyCursor));
        } else if (in instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.properties().keys();
        } else if (in instanceof VirtualRelationshipValue rel) {
            return extractKeys(access, access.relationshipPropertyIds(rel, relationshipScanCursor, propertyCursor));
        } else if (in instanceof MapValue) {
            return ((MapValue) in).keys();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'keys()': Expected a node, a relationship or a literal map but got %s",
                            in),
                    "keys",
                    in.prettify(),
                    List.of("NODE", "RELATIONSHIP", "LITERAL MAP"),
                    CypherTypeValueMapper.valueType(in));
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
        } else if (in instanceof NodeValue node && node.id() < 0) {
            return node.properties();
        } else if (in instanceof CompositeDatabaseValue.CompositeGraphDirectNodeValue node) {
            return node.properties();
        } else if (in instanceof VirtualNodeValue node) {
            return access.nodeAsMap(
                    node.id(), nodeCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else if (in instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.properties();
        } else if (in instanceof CompositeDatabaseValue.CompositeDirectRelationshipValue rel) {
            return rel.properties();
        } else if (in instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(
                    rel, relationshipCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else if (in instanceof MapValue) {
            return in;
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s",
                            in),
                    "properties",
                    in.prettify(),
                    List.of("NODE", "RELATIONSHIP", "LITERAL MAP"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static AnyValue properties(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            MapValueBuilder alreadyReadProperties,
            IntSet alreadyReadPropertyTokens) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NodeValue node && node.id() < 0) {
            return node.properties();
        } else if (in instanceof VirtualNodeValue node) {
            return access.nodeAsMap(
                    node.id(), nodeCursor, propertyCursor, alreadyReadProperties, alreadyReadPropertyTokens);
        } else if (in instanceof RelationshipValue rel && rel.id() < 0) {
            return rel.properties();
        } else if (in instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(
                    rel.id(), relationshipCursor, propertyCursor, alreadyReadProperties, alreadyReadPropertyTokens);
        } else if (in instanceof MapValue) {
            return in;
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s",
                            in),
                    "properties",
                    in.toString(),
                    List.of("NODE", "RELATIONSHIP", "LITERAL MAP"),
                    in.getTypeName());
        }
    }

    public static AnyValue characterLength(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue) {
            return size(item);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'character_length()': Expected a String, got: " + item,
                    "character_length",
                    item.prettify(),
                    List.of("STRING"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue vectorDimension(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VectorValue vector) {
            return longValue(vector.dimensions());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'vector_dimension_count()': Expected a VECTOR, got: " + item,
                    "vector_dimension_count",
                    item.prettify(),
                    List.of("VECTOR"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue size(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue textValue) {
            return longValue(textValue.length());
        } else if (item instanceof SequenceValue list) {
            return longValue(list.actualSize());
        } else if (item instanceof VectorValue vector) {
            return longValue(vector.dimensions());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'size()': Expected a String, Vector or List, got: " + item,
                    "size",
                    item.prettify(),
                    List.of("STRING", "VECTOR", "LIST<ANY>"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue sizeCypher5(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue textValue) {
            return longValue(textValue.length());
        } else if (item instanceof SequenceValue list) {
            return longValue(list.actualSize());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'size()': Expected a String or List, got: " + item,
                    "size",
                    item.prettify(),
                    List.of("STRING", "LIST<ANY>"),
                    CypherTypeValueMapper.valueType(item));
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
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'isEmpty()': Expected a List, Map, or String, got: " + item,
                    "isEmpty",
                    item.prettify(),
                    List.of("LIST<ANY>", "MAP", "STRING"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue length(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualPathValue) {
            return longValue(((VirtualPathValue) item).size());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'length()': Expected a Path, got: " + item,
                    "length",
                    item.prettify(),
                    List.of("PATH"),
                    CypherTypeValueMapper.valueType(item));
        }
    }

    public static AnyValue cardinality(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof SequenceValue list) {
            return longValue(list.actualSize());
        } else if (item instanceof VirtualPathValue path) {
            return longValue(path.cardinality());
        } else if (item instanceof MapValue map) {
            return longValue(map.size());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'cardinality()': Expected a Map, List or Path, got: " + item,
                    "cardinality",
                    item.prettify(),
                    List.of("MAP", "LIST<ANY>", "PATH"),
                    CypherTypeValueMapper.valueType(item));
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
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'toBoolean()': Expected a Boolean, Integer or String, got: " + in,
                    "toBoolean",
                    in.prettify(),
                    List.of("BOOLEAN", "INTEGER", "STRING"),
                    CypherTypeValueMapper.valueType(in));
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
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'toBooleanList()': Expected a List, got: %s", in),
                    "toBooleanList",
                    in.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static Value toFloat(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof DoubleValue d) {
            return d;
        } else if (in instanceof NumberValue number) {
            return doubleValue(number.doubleValue());
        } else if (in instanceof TextValue text) {
            return CypherRuntimeParser.parseAsDoubleOrElseNoValue(text.stringValue());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: " + in,
                    "toFloat",
                    in.prettify(),
                    List.of("STRING", "FLOAT", "INTEGER"),
                    CypherTypeValueMapper.valueType(in));
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
        } else if (in instanceof VectorValue v) {
            var list = ListValueBuilder.newListBuilder(v.dimensions());
            for (int i = 0; i < v.dimensions(); i++) {
                list.add(Values.doubleValue(v.doubleValue(i)));
            }
            return list.build();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'toFloatList()': Expected a List or Vector, got: %s", in),
                    "toFloatList",
                    in.prettify(),
                    List.of("LIST<ANY>", "VECTOR"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static AnyValue toFloatListCypher5(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toFloatOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'toFloatList()': Expected a List, got: %s", in),
                    "toFloatList",
                    in.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static Value toInteger(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralValue integer) {
            return integer;
        } else if (in instanceof NumberValue number) {
            return longValue(number.longValue());
        } else if (in instanceof TextValue text) {
            return CypherRuntimeParser.parseAsLongOrElseNoValue(text.stringValue());
        } else if (in instanceof BooleanValue bool) {
            if (bool.booleanValue()) {
                return longValue(1L);
            } else {
                return longValue(0L);
            }
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'toInteger()': Expected a String, Float, Integer or Boolean, got: "
                            + in,
                    "toInteger",
                    in.prettify(),
                    List.of("STRING", "FLOAT", "INTEGER", "BOOLEAN"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static Value toIntegerOrNull(AnyValue in) {
        if (in instanceof NumberValue || in instanceof BooleanValue) {
            return toInteger(in);
        } else if (in instanceof TextValue textValue) {
            try {
                return CypherRuntimeParser.parseAsLongOrElseNoValue(textValue.stringValue());
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
        } else if (in instanceof VectorValue v) {
            var list = ListValueBuilder.newListBuilder(v.dimensions());
            for (int i = 0; i < v.dimensions(); i++) {
                list.add(Values.longValue((long) v.doubleValue(i)));
            }
            return list.build();
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format(
                            "Invalid input for function 'toIntegerList()': Expected a List or Vector, got: %s", in),
                    "toIntegerList",
                    in.prettify(),
                    List.of("LIST<ANY>", "VECTOR"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static AnyValue toIntegerListCypher5(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralArray array) {
            return VirtualValues.fromArray(array);
        } else if (in instanceof FloatingPointArray array) {
            return toIntegerList(array);
        } else if (in instanceof SequenceValue sequence) {
            return toIntegerList(sequence);
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'toIntegerList()': Expected a List, got: %s", in),
                    "toIntegerList",
                    in.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static Value toString(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            return text;
        } else if (in instanceof UUIDValue uuidValue) {
            return stringValue(uuidValue.prettyPrint());
        } else if (in instanceof NumberValue number) {
            return stringValue(number.prettyPrint());
        } else if (in instanceof BooleanValue b) {
            return stringValue(b.prettyPrint());
        } else if (in instanceof TemporalValue || in instanceof DurationValue || in instanceof PointValue) {
            return stringValue(in.toString());
        } else {
            throw CypherTypeException.functionArgumentWrongType(
                    "Invalid input for function 'toString()': Expected a String, UUID, Float, Integer, Boolean, Temporal or Duration, got: "
                            + in,
                    "toString",
                    in.prettify(),
                    List.of("STRING", "UUID", "FLOAT", "INTEGER", "BOOLEAN", "TEMPORAL", "DURATION"),
                    CypherTypeValueMapper.valueType(in));
        }
    }

    public static AnyValue toStringOrNull(AnyValue in) {
        if (in instanceof TextValue
                || in instanceof NumberValue
                || in instanceof BooleanValue
                || in instanceof TemporalValue
                || in instanceof DurationValue
                || in instanceof PointValue
                || in instanceof UUIDValue) {
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
            throw CypherTypeException.functionArgumentWrongType(
                    String.format("Invalid input for function 'toStringList()': Expected a List, got: %s", in),
                    "toStringList",
                    in.prettify(),
                    List.of("LIST<ANY>"),
                    CypherTypeValueMapper.valueType(in));
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
            return list.drop(list.actualSize() + from);
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
            return list.take(list.intSize() + from);
        }
    }

    public static AnyValue fullSlice(AnyValue collection, AnyValue fromValue, AnyValue toValue) {
        if (collection == NO_VALUE || fromValue == NO_VALUE || toValue == NO_VALUE) {
            return NO_VALUE;
        }
        int from = asIntExact(fromValue);
        int to = asIntExact(toValue);
        ListValue list = asList(collection);
        int size = list.intSize();
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
                errorMessage = String.format(
                        "Expected %s to be a %s, but it was a %s",
                        value, TextValue.class.getName(), value.getClass().getName());
            } else {
                errorMessage = String.format(
                        "%s: Expected %s to be a %s, but it was a %s",
                        contextForErrorMessage.get(),
                        value,
                        TextValue.class.getName(),
                        value.getClass().getName());
            }

            throw CypherTypeException.expectedString(
                    errorMessage, value.prettyPrint(), CypherTypeValueMapper.valueType(value));
        }
        return (TextValue) value;
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
            throw CypherTypeException.nonIntegerListIndex(
                    String.valueOf(number), number.prettyPrint(), CypherTypeValueMapper.valueType(number));
        }
        long idx = number.longValue();

        if (idx < 0) {
            idx = container.actualSize() + idx;
        }
        if (idx >= container.actualSize() || idx < 0) {
            return NO_VALUE;
        }
        return container.value(idx);
    }

    private static int propertyKeyId(DbAccess dbAccess, AnyValue index) {
        return dbAccess.propertyKey(propertyKeyName(index));
    }

    private static String propertyKeyName(AnyValue index) {
        return asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot use a property key with non string name. It was " + index.toString());
    }

    private static AnyValue mapAccess(MapValue container, AnyValue index) {
        return container.get(asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot access a map '" + container + "' by key '" + index.toString() + "'"));
    }

    public static String asString(AnyValue value) {
        return asTextValue(value).stringValue();
    }

    public static List<String> nodeLabelsAsStringList(AnyValue value) {
        if (value instanceof TextValue text) {
            return Collections.singletonList(text.stringValue());
        } else if (value instanceof SequenceValue sequenceValue) {
            List<String> result = new ArrayList<>();
            for (var s : sequenceValue) {
                if (s instanceof TextValue t) {
                    result.add(t.stringValue());
                } else {
                    throw CypherTypeException.expectedStringNotNull(
                            "Expected node label to be a string or list of strings.",
                            s == NO_VALUE ? "NULL" : s.prettyPrint(),
                            CypherTypeValueMapper.valueType(s));
                }
            }
            return result;
        } else {
            throw CypherTypeException.expectedStringOrListOfStringsNotNull(
                    "Expected node label to be a string or list of strings.",
                    value == NO_VALUE ? "NULL" : value.prettyPrint(),
                    CypherTypeValueMapper.valueType(value));
        }
    }

    private static String asString(AnyValue value, Supplier<String> contextForErrorMessage) {
        return asTextValue(value, contextForErrorMessage).stringValue();
    }

    private static NumberValue asNumberValue(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (!(value instanceof NumberValue)) {
            var msg = String.format(
                    "%s: Expected %s to be a %s, but it was a %s",
                    contextForErrorMessage.get(),
                    value,
                    NumberValue.class.getName(),
                    value.getClass().getName());

            throw CypherTypeException.expectedNumber(msg, value.prettyPrint(), CypherTypeValueMapper.valueType(value));
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

    public static long asLong(AnyValue value) {
        return asLong(value, null, null, true);
    }

    private static long asLong(AnyValue value, Supplier<String> contextForErrorMessage, String functionName) {
        return asLong(value, contextForErrorMessage, functionName, true);
    }

    private static long asLong(
            AnyValue value, Supplier<String> contextForErrorMessage, String functionName, Boolean allowFloats) {
        if (value instanceof NumberValue numberValue && (allowFloats || !(numberValue instanceof FloatingPointValue))) {
            return numberValue.longValue();
        } else {
            String errorMsg;
            String numericTypeString = allowFloats ? "a numeric" : "an integer";
            if (contextForErrorMessage == null) {
                errorMsg = "Expected " + numericTypeString + " value but got: " + value;
            } else {
                errorMsg =
                        contextForErrorMessage.get() + ": Expected " + numericTypeString + " value but got: " + value;
            }
            if (functionName != null && !functionName.isEmpty()) {
                throw CypherTypeException.functionArgumentWrongType(
                        errorMsg,
                        functionName,
                        value.prettify(),
                        allowFloats ? List.of("INTEGER", "FLOAT") : List.of("INTEGER"),
                        CypherTypeValueMapper.valueType(value));
            } else {
                throw CypherTypeException.expectedNumber(
                        errorMsg, value.prettyPrint(), CypherTypeValueMapper.valueType(value));
            }
        }
    }

    public static int asNonNegativeIntExact(AnyValue value) {
        int result = asIntExact(value, null, null, true);
        if (result < 0) {
            throw InvalidArgumentException.countNotPosInt(result);
        }
        return result;
    }

    public static int asIntExact(AnyValue value) {
        return asIntExact(value, null, null, true);
    }

    public static int asIntExact(AnyValue value, Supplier<String> contextForErrorMessage, String functionName) {
        return asIntExact(value, contextForErrorMessage, functionName, true);
    }

    public static int asIntExact(
            AnyValue value, Supplier<String> contextForErrorMessage, String functionName, Boolean allowFloats) {
        return asIntExact(value, contextForErrorMessage, functionName, allowFloats, Integer.MIN_VALUE);
    }

    public static int asIntExact(
            AnyValue value,
            Supplier<String> contextForErrorMessage,
            String functionName,
            Boolean allowFloats,
            Integer minValue) {
        final long longValue = asLong(value, contextForErrorMessage, functionName, allowFloats);
        final int intValue = (int) longValue;
        if (intValue != longValue) {
            String errorMsg = String.format(
                    "Expected an integer between %d and %d, but got: %d", minValue, Integer.MAX_VALUE, longValue);
            if (contextForErrorMessage != null) {
                errorMsg = contextForErrorMessage.get() + ": " + errorMsg;
            }
            throw InvalidArgumentException.integerNonNullOutOfBounds(
                    errorMsg, "LIMIT", 0, Integer.MAX_VALUE, value.prettyPrint());
        }
        return intValue;
    }

    public static long nodeId(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (value instanceof VirtualNodeValue node) {
            return node.id();
        } else {
            throw CypherTypeException.expectedVirtualNode(
                    value.prettyPrint(), value.getClass().getName(), CypherTypeValueMapper.valueType(value));
        }
    }

    public static long nodeIdOrParameterWrongTypeError(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (value instanceof VirtualNodeValue node) {
            return node.id();
        } else {
            throw ParameterWrongTypeException.expectedNodeFoundInstead(
                    value.toString(), value.prettyPrint(), CypherTypeValueMapper.valueType(value));
        }
    }

    public static long relIdOrParameterWrongTypeError(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (value instanceof VirtualRelationshipValue rel) {
            return rel.id();
        } else {
            throw ParameterWrongTypeException.expectedNodeFoundInstead(
                    value.toString(), value.prettyPrint(), CypherTypeValueMapper.valueType(value));
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
                throw InvalidArgumentException.unknownNormalForm(String.valueOf(normalForm));
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
        } else if (typeName.couldBeStoredInProperty() && !(typeName instanceof ClosedDynamicUnionType)) {
            result = possibleValueRepresentations(typeName).contains(item.valueRepresentation());
            if (result && typeName instanceof VectorType vectorType && item instanceof VectorValue vectorValue) {
                // If the inner type was matched above, we also need to make sure the dimension is checked
                if (vectorType.dimension().isDefined()) {
                    long dimension = (Long) vectorType.dimension().get();
                    if (vectorValue.dimensions() != dimension) {
                        result = false;
                    }
                }
            }
        } else if (typeName instanceof NodeType) {
            result = item instanceof VirtualNodeValue;
        } else if (typeName instanceof RelationshipType) {
            result = item instanceof VirtualRelationshipValue;
        } else if (typeName instanceof MapType) {
            result = item instanceof MapValue;
        } else if (typeName instanceof PathType) {
            result = item instanceof VirtualPathValue;
        } else if (typeName instanceof VectorType vectorType) {
            // If we get here, the given Vector type is a super type of either VECTOR or VECTOR(dimension)
            result = item instanceof VectorValue vectorVal
                    && (vectorType.dimension().contains(vectorVal.dimensions())
                            || !vectorType.dimension().isDefined());
        } else if (typeName instanceof PropertyValueCypher5Type) {
            result = hasPropertyValueRepresentation(item.valueRepresentation(), false)
                    || (item instanceof ListValue listValue
                            && (listValue.isEmpty()
                                    || hasPropertyValueRepresentation(listValue.itemValueRepresentation(), true)));
        } else if (typeName instanceof PropertyValueType) {
            result = hasPropertyValueRepresentation(item.valueRepresentation(), false)
                    || (item instanceof ListValue listValue
                            && (listValue.isEmpty()
                                    || hasPropertyValueRepresentation(listValue.itemValueRepresentation(), true)));
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

    public static final CypherTypeValueMapper CYPHER_TYPE_NAME_VALUE_MAPPER = new CypherTypeValueMapper();

    public static Value valueType(AnyValue in) {
        return Values.stringValue(
                CypherType.normalizeTypes(in.map(CYPHER_TYPE_NAME_VALUE_MAPPER)).description());
    }

    private static boolean hasPropertyValueRepresentation(ValueRepresentation valueRepresentation, Boolean inList) {
        return !valueRepresentation.equals(ValueRepresentation.ANYTHING)
                && !valueRepresentation.equals(ValueRepresentation.UNKNOWN)
                && !valueRepresentation.equals(ValueRepresentation.NO_VALUE)
                // Vectors are property values, but not vectors inside of lists
                && !(isVectorValueRepresentation(valueRepresentation) && inList);
    }

    private static boolean isVectorValueRepresentation(ValueRepresentation valueRepresentation) {
        return valueRepresentation.equals(ValueRepresentation.FLOAT64_VECTOR)
                || valueRepresentation.equals(ValueRepresentation.FLOAT32_VECTOR)
                || valueRepresentation.equals(ValueRepresentation.INT64_VECTOR)
                || valueRepresentation.equals(ValueRepresentation.INT32_VECTOR)
                || valueRepresentation.equals(ValueRepresentation.INT16_VECTOR)
                || valueRepresentation.equals(ValueRepresentation.INT8_VECTOR);
    }

    private static final Map<Class<? extends CypherType>, List<ValueRepresentation>> TYPE_TO_REPRESENTATIONS =
            Map.ofEntries(
                    Map.entry(BooleanType.class, List.of(ValueRepresentation.BOOLEAN)),
                    Map.entry(StringType.class, List.of(ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT)),
                    Map.entry(UUIDType.class, List.of(ValueRepresentation.UUID)),
                    Map.entry(
                            IntegerType.class,
                            List.of(
                                    ValueRepresentation.INT8,
                                    ValueRepresentation.INT16,
                                    ValueRepresentation.INT32,
                                    ValueRepresentation.INT64)),
                    Map.entry(FloatType.class, List.of(ValueRepresentation.FLOAT32, ValueRepresentation.FLOAT64)),
                    Map.entry(
                            NumberType.class,
                            List.of(
                                    ValueRepresentation.INT8,
                                    ValueRepresentation.INT16,
                                    ValueRepresentation.INT32,
                                    ValueRepresentation.INT64,
                                    ValueRepresentation.FLOAT32,
                                    ValueRepresentation.FLOAT64)),
                    Map.entry(DateType.class, List.of(ValueRepresentation.DATE)),
                    Map.entry(LocalTimeType.class, List.of(ValueRepresentation.LOCAL_TIME)),
                    Map.entry(ZonedTimeType.class, List.of(ValueRepresentation.ZONED_TIME)),
                    Map.entry(LocalDateTimeType.class, List.of(ValueRepresentation.LOCAL_DATE_TIME)),
                    Map.entry(ZonedDateTimeType.class, List.of(ValueRepresentation.ZONED_DATE_TIME)),
                    Map.entry(DurationType.class, List.of(ValueRepresentation.DURATION)),
                    Map.entry(PointType.class, List.of(ValueRepresentation.GEOMETRY)),
                    Map.entry(GeometryType.class, List.of(ValueRepresentation.GEOMETRY)));

    private static final Map<Class<? extends CypherType>, List<ValueRepresentation>>
            LIST_INNER_TYPE_TO_REPRESENTATIONS = Map.ofEntries(
                    Map.entry(BooleanType.class, List.of(ValueRepresentation.BOOLEAN_ARRAY)),
                    Map.entry(StringType.class, List.of(ValueRepresentation.TEXT_ARRAY)),
                    Map.entry(UUIDType.class, List.of(ValueRepresentation.UUID_ARRAY)),
                    Map.entry(
                            IntegerType.class,
                            List.of(
                                    ValueRepresentation.INT8_ARRAY,
                                    ValueRepresentation.INT16_ARRAY,
                                    ValueRepresentation.INT32_ARRAY,
                                    ValueRepresentation.INT64_ARRAY)),
                    Map.entry(
                            FloatType.class,
                            List.of(ValueRepresentation.FLOAT32_ARRAY, ValueRepresentation.FLOAT64_ARRAY)),
                    Map.entry(
                            NumberType.class,
                            List.of(
                                    ValueRepresentation.INT8_ARRAY,
                                    ValueRepresentation.INT16_ARRAY,
                                    ValueRepresentation.INT32_ARRAY,
                                    ValueRepresentation.INT64_ARRAY,
                                    ValueRepresentation.FLOAT32_ARRAY,
                                    ValueRepresentation.FLOAT64_ARRAY)),
                    Map.entry(DateType.class, List.of(ValueRepresentation.DATE_ARRAY)),
                    Map.entry(LocalTimeType.class, List.of(ValueRepresentation.LOCAL_TIME_ARRAY)),
                    Map.entry(ZonedTimeType.class, List.of(ValueRepresentation.ZONED_TIME_ARRAY)),
                    Map.entry(LocalDateTimeType.class, List.of(ValueRepresentation.LOCAL_DATE_TIME_ARRAY)),
                    Map.entry(ZonedDateTimeType.class, List.of(ValueRepresentation.ZONED_DATE_TIME_ARRAY)),
                    Map.entry(DurationType.class, List.of(ValueRepresentation.DURATION_ARRAY)),
                    Map.entry(GeometryType.class, List.of(ValueRepresentation.GEOMETRY_ARRAY)),
                    Map.entry(PointType.class, List.of(ValueRepresentation.GEOMETRY_ARRAY)));

    private static List<ValueRepresentation> possibleValueRepresentations(CypherType cypherType)
            throws UnsupportedOperationException {

        // Direct mapping for standard types
        List<ValueRepresentation> representations = TYPE_TO_REPRESENTATIONS.get(cypherType.getClass());
        if (representations != null) {
            return representations;
        }

        // Handle VectorType specially
        if (cypherType instanceof VectorType vectorType) {
            if (vectorType.innerType().isDefined()) {
                return switch (vectorType.innerType().get()) {
                    case Float32Type __ -> List.of(ValueRepresentation.FLOAT32_VECTOR);
                    case FloatType __ -> List.of(ValueRepresentation.FLOAT64_VECTOR);
                    case Integer8Type __ -> List.of(ValueRepresentation.INT8_VECTOR);
                    case Integer16Type __ -> List.of(ValueRepresentation.INT16_VECTOR);
                    case Integer32Type __ -> List.of(ValueRepresentation.INT32_VECTOR);
                    case IntegerType __ -> List.of(ValueRepresentation.INT64_VECTOR);
                    default ->
                        throw new UnsupportedOperationException("Unsupported vector coordinate type: "
                                + vectorType.innerType().get().getClass().getName());
                };
            } else {
                return List.of(
                        ValueRepresentation.FLOAT64_VECTOR,
                        ValueRepresentation.FLOAT32_VECTOR,
                        ValueRepresentation.INT64_VECTOR,
                        ValueRepresentation.INT32_VECTOR,
                        ValueRepresentation.INT16_VECTOR,
                        ValueRepresentation.INT8_VECTOR);
            }
        }

        // Handle ListType specially
        if (cypherType instanceof ListType listType) {
            CypherType innerType = listType.innerType();
            if (innerType instanceof ClosedDynamicUnionType closedDynamicUnionType) {
                return possibleUnionListValueRepresentations(closedDynamicUnionType);
            }
            List<ValueRepresentation> listReps = LIST_INNER_TYPE_TO_REPRESENTATIONS.get(innerType.getClass());
            return listReps != null ? listReps : List.of();
        }

        // Handle closed dynamic union types specially
        if (cypherType instanceof ClosedDynamicUnionType closedDynamicUnionType) {
            return possibleUnionValueRepresentations(closedDynamicUnionType);
        }

        throw new UnsupportedOperationException("possibleValueRepresentations not supported on "
                + cypherType.getClass().getName());
    }

    private static List<ValueRepresentation> possibleUnionValueRepresentations(ClosedDynamicUnionType innerListType) {
        List<ValueRepresentation> possibleValueRepresentations = new ArrayList<>();
        for (CypherType cypherType : asJava(innerListType.innerTypes())) {
            List<ValueRepresentation> representations = possibleValueRepresentations(cypherType);
            if (representations != null) {
                possibleValueRepresentations.addAll(representations);
            }
        }
        return possibleValueRepresentations;
    }

    private static List<ValueRepresentation> possibleUnionListValueRepresentations(
            ClosedDynamicUnionType innerListType) {
        List<ValueRepresentation> possibleValueRepresentations = new ArrayList<>();
        for (CypherType cypherType : asJava(innerListType.innerTypes())) {
            List<ValueRepresentation> representations = LIST_INNER_TYPE_TO_REPRESENTATIONS.get(cypherType.getClass());
            if (representations != null) {
                possibleValueRepresentations.addAll(representations);
            }
        }
        return possibleValueRepresentations;
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
                    || itemType instanceof PropertyValueCypher5Type
                    || (typeName.couldBeStoredInProperty()
                            && possibleValueRepresentations(typeName).contains(array.valueRepresentation()));
        } else if (values instanceof ListValue list) {
            // For a simple LIST<TYPE NOT NULL> we can quickly check the list type
            // without needing to iterate over the list
            // Lists that are mixed ints and floats will return as a list of float here, so don't allow the shortcut for
            // that
            if (itemType.couldBeStoredInProperty()
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
            throw CypherTypeException.expectedNode(
                    String.valueOf(item), item.prettyPrint(), CypherTypeValueMapper.valueType(item));
        }
    }

    private static CypherTypeException needsNumbers(String method, AnyValue in) {
        return CypherTypeException.functionArgumentWrongType(
                String.format("%s() requires numbers", method),
                method,
                in.prettify(),
                List.of("INTEGER", "FLOAT"),
                CypherTypeValueMapper.valueType(in));
    }

    private static CypherTypeException notAString(String method, AnyValue in) {
        return CypherTypeException.functionArgumentWrongType(
                String.format(
                        "Expected a string value for `%s`, but got: %s; consider converting it to a string with "
                                + "toString().",
                        method, in),
                method,
                in.prettify(),
                List.of("STRING"),
                CypherTypeValueMapper.valueType(in));
    }

    private static CypherTypeException notAVector(String method, AnyValue in) {
        return CypherTypeException.functionArgumentWrongType(
                String.format("Expected a vector value for `%s`, but got: %s.", method, in),
                method,
                in.prettify(),
                List.of("VECTOR"),
                CypherTypeValueMapper.valueType(in));
    }

    private static CypherTypeException notAModeString(String method, AnyValue mode) {
        return CypherTypeException.functionArgumentWrongType(
                String.format("Expected a string value for `%s`, but got: %s.", method, mode),
                method,
                mode.prettify(),
                List.of("STRING"),
                CypherTypeValueMapper.valueType(mode));
    }

    private static ListValue toIntegerList(FloatingPointArray array) {
        var converted = ListValueBuilder.newListBuilder(array.intSize());
        for (int i = 0; i < array.intSize(); i++) {
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
