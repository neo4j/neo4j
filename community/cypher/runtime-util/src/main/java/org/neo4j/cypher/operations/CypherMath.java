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

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.List;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This class contains static helper math methods used by the compiled expressions
 */
@SuppressWarnings({"ReferenceEquality"})
public final class CypherMath {
    private CypherMath() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    // TODO this is horrible spaghetti code, we should push most of this down to AnyValue
    public static AnyValue add(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        }

        if (lhs instanceof NumberValue l && rhs instanceof NumberValue r) {
            try {
                return l.plus(r);
            } catch (java.lang.ArithmeticException e) {
                throw ArithmeticException.wrappedArithmeticException(lhs.prettify() + " + " + rhs.prettify(), "+", e);
            }
        }
        // List addition
        // arrays are same as lists when it comes to addition
        if (lhs instanceof ArrayValue array) {
            lhs = VirtualValues.fromArray(array);
        }
        if (rhs instanceof ArrayValue array) {
            rhs = VirtualValues.fromArray(array);
        }

        if (lhs instanceof ListValue lhsList && rhs instanceof ListValue rhsList) {
            return lhsList.appendAll(rhsList);
        } else if (lhs instanceof ListValue lhsList) {
            return lhsList.append(rhs);
        } else if (rhs instanceof ListValue rhsList) {
            return rhsList.prepend(lhs);
        }

        // String addition
        if (lhs instanceof TextValue lhsText && rhs instanceof TextValue rhsText) {
            return lhsText.plus(rhsText);
        } else if (lhs instanceof TextValue lhsText) {
            if (rhs instanceof Value) {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if (!(rhs instanceof TemporalValue || rhs instanceof DurationValue || rhs instanceof PointValue)) {
                    return stringValue((lhsText).stringValue() + rhs.prettyPrint());
                } else {
                    return stringValue((lhsText).stringValue() + rhs);
                }
            }
        } else if (rhs instanceof TextValue rhsText) {
            if (lhs instanceof Value) {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if (!(lhs instanceof TemporalValue || lhs instanceof DurationValue || lhs instanceof PointValue)) {
                    return stringValue(lhs.prettyPrint() + (rhsText).stringValue());
                } else {
                    return stringValue(lhs + rhsText.stringValue());
                }
            }
        }

        // Temporal values
        if (lhs instanceof TemporalValue<?, ?> lhsTemporal && rhs instanceof DurationValue rhsDuration) {
            return lhsTemporal.plus(rhsDuration);
        }
        if (lhs instanceof DurationValue lhsDuration) {
            if (rhs instanceof TemporalValue<?, ?> rhsTemporal) {
                return rhsTemporal.plus(lhsDuration);
            }
            if (rhs instanceof DurationValue rhsDuration) {
                return lhsDuration.add(rhsDuration);
            }
        }

        // No matching case — build the expectedTypes list specific to the lhs type and throw.
        final List<String> expectedTypes;
        if (lhs instanceof TemporalValue<?, ?>) {
            expectedTypes =
                    List.of(StringValue.CYPHER_TYPE_NAME, DurationValue.CYPHER_TYPE_NAME, ListValue.CYPHER_TYPE_NAME);
        } else if (lhs instanceof DurationValue) {
            expectedTypes = List.of(
                    StringValue.CYPHER_TYPE_NAME,
                    DurationValue.CYPHER_TYPE_NAME,
                    DateValue.CYPHER_TYPE_NAME,
                    TimeValue.CYPHER_TYPE_NAME,
                    LocalTimeValue.CYPHER_TYPE_NAME,
                    DateTimeValue.CYPHER_TYPE_NAME,
                    LocalDateTimeValue.CYPHER_TYPE_NAME,
                    ListValue.CYPHER_TYPE_NAME);
        } else if (lhs instanceof VirtualNodeValue
                || lhs instanceof VirtualRelationshipValue
                || lhs instanceof MapValue) {
            // Only lists can be added to nodes, relationships and maps.
            // Positive cases are covered under 'List addition' above.
            expectedTypes = List.of(ListValue.CYPHER_TYPE_NAME);
        } else {
            expectedTypes = List.of(
                    FloatingPointValue.CYPHER_TYPE_NAME,
                    IntegralValue.CYPHER_TYPE_NAME,
                    StringValue.CYPHER_TYPE_NAME,
                    ListValue.CYPHER_TYPE_NAME);
        }

        if (lhs == null) {
            throw CypherTypeException.addTypeMismatch(
                    rhs.prettyPrint(), "NULL", rhs.getTypeName(), CypherTypeValueMapper.valueType(rhs), expectedTypes);
        } else {
            throw CypherTypeException.addTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    expectedTypes);
        }
    }

    public static AnyValue subtract(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        }

        String expectedType = null;

        // numbers

        if (lhs instanceof NumberValue lhsNumber && rhs instanceof NumberValue rhsNumber) {
            try {
                return lhsNumber.minus(rhsNumber);
            } catch (java.lang.ArithmeticException e) {
                throw ArithmeticException.wrappedArithmeticException(lhs.prettify() + " - " + rhs.prettify(), "-", e);
            }
        }
        // Temporal values
        if (lhs instanceof TemporalValue<?, ?> lhsTemporal) {
            if (rhs instanceof DurationValue rhsDuration) {
                return lhsTemporal.minus(rhsDuration);
            }
            expectedType = DurationValue.CYPHER_TYPE_NAME;
        }
        if (lhs instanceof DurationValue lhsDuration) {
            if (rhs instanceof DurationValue rhsDuration) {
                return lhsDuration.sub(rhsDuration);
            }
            expectedType = DurationValue.CYPHER_TYPE_NAME;
        }

        if (lhs == null) {
            throw CypherTypeException.subtractTypeMismatch(
                    rhs.prettyPrint(),
                    "NULL",
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    "INTEGER | FLOAT or DURATION");
        } else {
            if (lhs instanceof NumberValue) {
                expectedType = "INTEGER | FLOAT";
            }
            if (expectedType != null) {
                throw CypherTypeException.subtractTypeMismatch(
                        rhs.prettyPrint(),
                        lhs.getTypeName(),
                        rhs.getTypeName(),
                        CypherTypeValueMapper.valueType(rhs),
                        expectedType);
            }
            throw CypherTypeException.subtractTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(lhs),
                    CypherTypeValueMapper.valueType(rhs));
        }
    }

    public static AnyValue multiply(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        }

        if (lhs instanceof NumberValue lhsNumber && rhs instanceof NumberValue rhsNumber) {
            try {
                return lhsNumber.times(rhsNumber);
            } catch (java.lang.ArithmeticException e) {
                throw ArithmeticException.wrappedArithmeticException(lhs.prettify() + " * " + rhs.prettify(), "*", e);
            }
        }
        // Temporal values
        if (lhs instanceof DurationValue lhsDuration && rhs instanceof NumberValue rhsNumber) {
            return lhsDuration.mul(rhsNumber);
        }
        if (rhs instanceof DurationValue rhsDuration && lhs instanceof NumberValue lhsNumber) {
            return rhsDuration.mul(lhsNumber);
        }

        // No matching case — build the expectedTypes list specific to the lhs type and throw.
        final List<String> expectedTypes;
        if (lhs instanceof DurationValue) {
            expectedTypes = List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME);
        } else {
            expectedTypes = List.of(
                    FloatingPointValue.CYPHER_TYPE_NAME,
                    IntegralValue.CYPHER_TYPE_NAME,
                    DurationValue.CYPHER_TYPE_NAME);
        }

        if (lhs == null) {
            throw CypherTypeException.multiplyTypeMismatch(
                    rhs.prettyPrint(), "NULL", rhs.getTypeName(), CypherTypeValueMapper.valueType(rhs), expectedTypes);
        } else {
            throw CypherTypeException.multiplyTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    expectedTypes);
        }
    }

    private static boolean divideCheckForNull(AnyValue lhs, AnyValue rhs) {
        if (rhs instanceof IntegralValue && rhs.equals(ZERO_INT)) {
            throw ArithmeticException.divisionByZero();
        } else {
            return lhs == NO_VALUE || rhs == NO_VALUE;
        }
    }

    public static AnyValue divide(AnyValue lhs, AnyValue rhs) {
        if (divideCheckForNull(lhs, rhs)) {
            return NO_VALUE;
        }

        if (lhs instanceof NumberValue lhsNumber && rhs instanceof NumberValue rhsNumber) {
            return lhsNumber.divideBy(rhsNumber);
        }
        // Temporal values
        if (lhs instanceof DurationValue lhsDuration) {
            if (rhs instanceof NumberValue rhsNumber) {
                return lhsDuration.div(rhsNumber);
            }
        }

        if (lhs == null) {
            throw CypherTypeException.divideTypeMismatch(
                    rhs.prettyPrint(),
                    "NULL",
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        } else {
            throw CypherTypeException.divideTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        }
    }

    public static AnyValue modulo(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        } else if (lhs instanceof NumberValue lhsNumber && rhs instanceof NumberValue rhsNumber) {
            try {
                if (lhsNumber instanceof FloatingPointValue || rhsNumber instanceof FloatingPointValue) {
                    return doubleValue(lhsNumber.doubleValue() % rhsNumber.doubleValue());
                } else {
                    return longValue(lhsNumber.longValue() % rhsNumber.longValue());
                }
            } catch (java.lang.ArithmeticException e) {
                throw ArithmeticException.wrappedArithmeticException(lhs.prettify() + " % " + rhs.prettify(), "%", e);
            }
        }

        if (lhs == null) {
            throw CypherTypeException.modulusTypeMismatch(
                    rhs.prettyPrint(),
                    "NULL",
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        } else {
            throw CypherTypeException.modulusTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        }
    }

    public static AnyValue pow(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        } else if (lhs instanceof NumberValue lhsNumber && rhs instanceof NumberValue rhsNumber) {
            return doubleValue(Math.pow(lhsNumber.doubleValue(), rhsNumber.doubleValue()));
        }

        if (lhs == null) {
            throw CypherTypeException.powerTypeMismatch(
                    rhs.prettyPrint(),
                    "NULL",
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        } else {
            throw CypherTypeException.powerTypeMismatch(
                    rhs.prettyPrint(),
                    lhs.getTypeName(),
                    rhs.getTypeName(),
                    CypherTypeValueMapper.valueType(rhs),
                    List.of(FloatingPointValue.CYPHER_TYPE_NAME, IntegralValue.CYPHER_TYPE_NAME));
        }
    }
}
