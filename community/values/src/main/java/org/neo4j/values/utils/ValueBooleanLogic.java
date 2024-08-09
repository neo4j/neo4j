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
package org.neo4j.values.utils;

import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.virtual.VirtualValues.asList;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValues;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * This class contains static helper boolean methods for performing boolean logic on values
 */
@SuppressWarnings({"ReferenceEquality"})
public final class ValueBooleanLogic {
    private static final BooleanMapper BOOLEAN_MAPPER = new BooleanMapper();

    private ValueBooleanLogic() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    @CalledFromGeneratedCode
    public static BooleanValue xor(BooleanValue lhs, BooleanValue rhs) {
        return (lhs == TRUE) ^ (rhs == TRUE) ? TRUE : FALSE;
    }

    @CalledFromGeneratedCode
    public static Value not(AnyValue in) {
        assert in == NO_VALUE || in == TRUE || in == FALSE;
        return in == NO_VALUE ? NO_VALUE : (in != TRUE ? TRUE : FALSE);
    }

    @CalledFromGeneratedCode
    public static Value equals(AnyValue lhs, AnyValue rhs) {
        Equality compare = lhs.ternaryEquals(rhs);
        return switch (compare) {
            case TRUE -> Values.TRUE;
            case FALSE -> Values.FALSE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static Value notEquals(AnyValue lhs, AnyValue rhs) {
        Equality compare = lhs.ternaryEquals(rhs);
        return switch (compare) {
            case TRUE -> Values.FALSE;
            case FALSE -> Values.TRUE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static BooleanValue regex(TextValue lhs, TextValue rhs) {
        assert lhs != NO_VALUE && rhs != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        String regexString = rhs.stringValue();
        try {
            boolean matches =
                    Pattern.compile(regexString).matcher(lhs.stringValue()).matches();
            return matches ? TRUE : FALSE;
        } catch (PatternSyntaxException e) {
            throw new InvalidSemanticsException("Invalid Regex: " + e.getMessage(), null);
        }
    }

    @CalledFromGeneratedCode
    public static BooleanValue regex(TextValue text, Pattern pattern) {
        assert text != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        boolean matches = pattern.matcher(text.stringValue()).matches();
        return matches ? TRUE : FALSE;
    }

    @CalledFromGeneratedCode
    public static Value lessThan(AnyValue lhs, AnyValue rhs) {
        if (AnyValue.isNanAndNumber(lhs, rhs)) {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare(lhs, rhs);
        return switch (comparison) {
            case EQUAL -> {
                if (lhs.isIncomparableType()) {
                    yield NO_VALUE;
                } else {
                    yield FALSE;
                }
            }
            case GREATER_THAN -> FALSE;
            case SMALLER_THAN -> TRUE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static Value lessThanOrEqual(AnyValue lhs, AnyValue rhs) {
        if (AnyValue.isNanAndNumber(lhs, rhs)) {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare(lhs, rhs);
        return switch (comparison) {
            case GREATER_THAN -> FALSE;
            case EQUAL, SMALLER_THAN -> TRUE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static Value greaterThan(AnyValue lhs, AnyValue rhs) {
        if (AnyValue.isNanAndNumber(lhs, rhs)) {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare(lhs, rhs);
        return switch (comparison) {
            case GREATER_THAN -> TRUE;
            case EQUAL -> {
                if (lhs.isIncomparableType()) {
                    yield NO_VALUE;
                } else {
                    yield FALSE;
                }
            }
            case SMALLER_THAN -> FALSE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static Value greaterThanOrEqual(AnyValue lhs, AnyValue rhs) {
        if (AnyValue.isNanAndNumber(lhs, rhs)) {
            return FALSE;
        }
        Comparison comparison = AnyValues.TERNARY_COMPARATOR.ternaryCompare(lhs, rhs);
        return switch (comparison) {
            case GREATER_THAN, EQUAL -> TRUE;
            case SMALLER_THAN -> FALSE;
            case UNDEFINED -> NO_VALUE;
        };
    }

    @CalledFromGeneratedCode
    public static Value coerceToBoolean(AnyValue value) {
        return value.map(BOOLEAN_MAPPER);
    }

    @CalledFromGeneratedCode
    public static Value in(AnyValue findMe, AnyValue lookIn) {
        if (lookIn == NO_VALUE) {
            return NO_VALUE;
        }

        ListValue list = asList(lookIn);

        if (list.isEmpty()) {
            return BooleanValue.FALSE;
        }

        if (findMe == NO_VALUE) {
            return NO_VALUE;
        }

        return list.ternaryContains(findMe);
    }

    @CalledFromGeneratedCode
    public static Value in(AnyValue findMe, AnyValue lookIn, InCache cache, MemoryTracker memoryTracker) {
        if (lookIn == NO_VALUE) {
            return NO_VALUE;
        }

        return cache.check(findMe, asList(lookIn), memoryTracker);
    }

    private static final class BooleanMapper implements ValueMapper<Value> {
        @Override
        public Value mapPath(VirtualPathValue value) {
            return value.size() > 0 ? TRUE : FALSE;
        }

        @Override
        public Value mapNode(VirtualNodeValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapRelationship(VirtualRelationshipValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapMap(MapValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapNoValue() {
            return NO_VALUE;
        }

        @Override
        public Value mapSequence(SequenceValue value) {
            return value.intSize() > 0 ? TRUE : FALSE;
        }

        @Override
        public Value mapText(TextValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapBoolean(BooleanValue value) {
            return value;
        }

        @Override
        public Value mapNumber(NumberValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapDateTime(DateTimeValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapLocalDateTime(LocalDateTimeValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapDate(DateValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapTime(TimeValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapLocalTime(LocalTimeValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapDuration(DurationValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }

        @Override
        public Value mapPoint(PointValue value) {
            throw new CypherTypeException("Don't know how to treat that as a boolean: " + value);
        }
    }
}
