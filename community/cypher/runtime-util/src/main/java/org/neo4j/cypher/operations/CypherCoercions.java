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

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTTime;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

import java.util.Map;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
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
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

@SuppressWarnings({"WeakerAccess"})
public final class CypherCoercions {
    private CypherCoercions() {
        throw new UnsupportedOperationException("do not instantiate");
    }

    /**
     * Attempts to create a storable value of else fails with a type exception.
     */
    public static Value asStorableValue(AnyValue anyValue) {
        if (anyValue instanceof Value value) {
            return value;
        } else if (anyValue instanceof ListValue list) {
            return list.toStorableArray();
        } else {
            throw new CypherTypeException(
                    "Property values can only be of primitive types or arrays thereof. Encountered: " + anyValue + ".");
        }
    }

    public static Value asStorableValueOrNull(AnyValue anyValue) {
        if (anyValue instanceof Value value) {
            return value;
        } else if (anyValue instanceof ListValue list
                && (list.isEmpty() || list.itemValueRepresentation().canCreateArrayOfValueGroup())) {
            return list.toStorableArray();
        } else {
            return null;
        }
    }

    public static AnyValue asTextValueOrNull(AnyValue value) {
        if (value instanceof TextValue text) {
            return text;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "String");
    }

    public static AnyValue asNodeValueOrNull(AnyValue value) {
        if (value instanceof VirtualNodeValue node) {
            return node;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Node");
    }

    public static AnyValue asRelationshipValueOrNull(AnyValue value) {
        if (value instanceof VirtualRelationshipValue rel) {
            return rel;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Relationship");
    }

    public static AnyValue asPathValueOrNull(AnyValue value) {
        if (value instanceof VirtualPathValue path) {
            return path;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Path");
    }

    public static AnyValue asIntegralValueOrNull(AnyValue value) {
        if (value instanceof NumberValue number) {
            return Values.longValue(number.longValue());
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Integer");
    }

    public static AnyValue asFloatingPointValueOrNull(AnyValue value) {
        if (value instanceof NumberValue number) {
            return Values.doubleValue(number.doubleValue());
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Float");
    }

    public static AnyValue asBooleanValueOrNull(AnyValue value) {
        if (value instanceof BooleanValue booleanValue) {
            return booleanValue;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Boolean");
    }

    public static NumberValue asNumberValue(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (!(value instanceof NumberValue)) {
            throw cantCoerce(value, "Number");
        }
        return (NumberValue) value;
    }

    public static AnyValue asNumberValueOrNull(AnyValue value) {
        if (value instanceof NumberValue numberValue) {
            return numberValue;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Number");
    }

    public static AnyValue asPointValueOrNull(AnyValue value) {
        if (value instanceof PointValue point) {
            return point;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Point");
    }

    public static AnyValue asDateValueOrNull(AnyValue value) {
        if (value instanceof DateValue date) {
            return date;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Date");
    }

    public static AnyValue asTimeValueOrNull(AnyValue value) {
        if (value instanceof TimeValue time) {
            return time;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Time");
    }

    public static AnyValue asLocalTimeValueOrNull(AnyValue value) {
        if (value instanceof LocalTimeValue time) {
            return time;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "LocalTime");
    }

    public static AnyValue asLocalDateTimeValueOrNull(AnyValue value) {
        if (value instanceof LocalDateTimeValue time) {
            return time;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "LocalDateTime");
    }

    public static AnyValue asDateTimeValueOrNull(AnyValue value) {
        if (value instanceof DateTimeValue date) {
            return date;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "DateTime");
    }

    public static AnyValue asDurationValueOrNull(AnyValue value) {
        if (value instanceof DurationValue duration) {
            return duration;
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        }
        throw cantCoerce(value, "Duration");
    }

    public static MapValue asMapValue(
            AnyValue value,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor) {
        if (value instanceof MapValue map) {
            return map;
        } else if (value instanceof NodeValue.DirectNodeValue node) {
            return node.properties();
        } else if (value instanceof VirtualNodeValue node) {
            return access.nodeAsMap(
                    node.id(), nodeCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else if (value instanceof RelationshipValue.DirectRelationshipValue rel) {
            return rel.properties();
        } else if (value instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(
                    rel.id(), relationshipCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else {
            throw cantCoerce(value, "Map");
        }
    }

    public static AnyValue asMapValueOrNull(
            AnyValue value,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor) {
        return value == NO_VALUE ? NO_VALUE : asMapValue(value, access, nodeCursor, relationshipCursor, propertyCursor);
    }

    public static SequenceValue asSequenceValue(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (!(value instanceof SequenceValue)) {
            throw cantCoerce(value, "SequenceValue");
        }
        return (SequenceValue) value;
    }

    static CypherTypeException cantCoerce(AnyValue value, String type) {
        return new CypherTypeException(format("Can't coerce `%s` to %s", value, type));
    }

    public static Coercer coercerFromType(Neo4jTypes.AnyType type) {
        final var coercer = STATIC_CONVERTERS.get(type.getClass());
        if (coercer != null) {
            return coercer;
        } else if (type instanceof Neo4jTypes.ListType listType) {
            return new ListCoercer(listType.innerType());
        }
        throw new CypherTypeException(format("Can't coerce to type %s", type));
    }

    private static final Map<Class<? extends Neo4jTypes.AnyType>, CypherCoercions.Coercer> STATIC_CONVERTERS =
            Map.ofEntries(
                    Map.entry(NTAny.getClass(), (a, ignore2, cursors) -> a),
                    Map.entry(NTString.getClass(), (a, ignore2, cursors) -> asTextValueOrNull(a)),
                    Map.entry(NTNumber.getClass(), (a, ignore2, cursors) -> asNumberValueOrNull(a)),
                    Map.entry(NTInteger.getClass(), (a, ignore2, cursors) -> asIntegralValueOrNull(a)),
                    Map.entry(NTFloat.getClass(), (a, ignore2, cursors) -> asFloatingPointValueOrNull(a)),
                    Map.entry(NTBoolean.getClass(), (a, x, y) -> asBooleanValueOrNull(a)),
                    Map.entry(
                            NTMap.getClass(),
                            (a, c, cursors) -> asMapValueOrNull(
                                    a,
                                    c,
                                    cursors.nodeCursor(),
                                    cursors.relationshipScanCursor(),
                                    cursors.propertyCursor())),
                    Map.entry(NTNode.getClass(), (a, ignore2, cursors) -> asNodeValueOrNull(a)),
                    Map.entry(NTRelationship.getClass(), (a, ignore2, cursors) -> asRelationshipValueOrNull(a)),
                    Map.entry(NTPath.getClass(), (a, ignore2, cursors) -> asPathValueOrNull(a)),
                    Map.entry(NTGeometry.getClass(), (a, ignore2, cursors) -> asPointValueOrNull(a)),
                    Map.entry(NTPoint.getClass(), (a, ignore2, cursors) -> asPointValueOrNull(a)),
                    Map.entry(NTDateTime.getClass(), (a, ignore2, cursors) -> asDateTimeValueOrNull(a)),
                    Map.entry(NTLocalDateTime.getClass(), (a, ignore2, cursors) -> asLocalDateTimeValueOrNull(a)),
                    Map.entry(NTDate.getClass(), (a, ignore2, cursors) -> asDateValueOrNull(a)),
                    Map.entry(NTTime.getClass(), (a, ignore2, cursors) -> asTimeValueOrNull(a)),
                    Map.entry(NTLocalTime.getClass(), (a, ignore2, cursors) -> asLocalTimeValueOrNull(a)),
                    Map.entry(NTDuration.getClass(), (a, ignore2, cursors) -> asDurationValueOrNull(a)));

    /** Value coercer. All implementations must be immutable! They are reused. */
    @FunctionalInterface
    public interface Coercer {
        AnyValue apply(AnyValue value, DbAccess access, ExpressionCursors cursors);
    }
}

class ListCoercer implements CypherCoercions.Coercer {
    private final Neo4jTypes.AnyType innerType;
    private final CypherCoercions.Coercer innerCoercer;

    public ListCoercer(final Neo4jTypes.AnyType innerType) {
        this.innerType = innerType;
        this.innerCoercer = CypherCoercions.coercerFromType(innerType);
    }

    @Override
    public AnyValue apply(final AnyValue value, final DbAccess access, final ExpressionCursors cursors) {
        if (value instanceof SequenceValue sequence) {
            if (sequence.isEmpty()) {
                return EMPTY_LIST;
            } else if (sequence instanceof ArrayValue array && itemsHaveCorrectType(array, innerType)) {
                return VirtualValues.fromArray(array);
            } else if (sequence instanceof ListValue list && itemsHaveCorrectType(list, innerType)) {
                return list;
            } else {
                return slowCoersion(sequence, access, cursors);
            }
        } else if (innerType == NTAny && value instanceof VirtualPathValue path) {
            return path.asList();
        } else if (value instanceof MapValue map) {
            // Not sure why we have this, but we have old test cases for it
            return VirtualValues.list(innerCoercer.apply(map, access, cursors));
        } else if (value == NO_VALUE) {
            return NO_VALUE;
        } else {
            throw CypherCoercions.cantCoerce(value, "List");
        }
    }

    private ListValue slowCoersion(
            final SequenceValue sequence, final DbAccess access, final ExpressionCursors cursors) {
        if (sequence.iterationPreference() == RANDOM_ACCESS) {
            final int length = sequence.intSize();
            final var builder = ListValueBuilder.newListBuilder(length);
            for (int i = 0; i < length; i++) {
                builder.add(innerCoercer.apply(sequence.value(i), access, cursors));
            }
            return builder.build();
        } else {
            final var builder = ListValueBuilder.newListBuilder();
            for (final var item : sequence) {
                builder.add(innerCoercer.apply(item, access, cursors));
            }
            return builder.build();
        }
    }

    private static boolean itemsHaveCorrectType(final ArrayValue array, final Neo4jTypes.AnyType target) {
        return switch (array.valueRepresentation()) {
            case GEOMETRY_ARRAY -> target == NTGeometry;
            case ZONED_DATE_TIME_ARRAY -> target == NTDateTime;
            case LOCAL_DATE_TIME_ARRAY -> target == NTLocalDateTime;
            case DATE_ARRAY -> target == NTDate;
            case ZONED_TIME_ARRAY -> target == NTTime;
            case LOCAL_TIME_ARRAY -> target == NTLocalTime;
            case DURATION_ARRAY -> target == NTDuration;
            case TEXT_ARRAY -> target == NTString;
            case BOOLEAN_ARRAY -> target == NTBoolean;
            case INT64_ARRAY -> target == NTInteger || target == NTNumber;
            case FLOAT64_ARRAY -> target == NTFloat || target == NTNumber;
            case INT32_ARRAY, INT16_ARRAY, INT8_ARRAY, FLOAT32_ARRAY -> target == NTNumber;
            default -> false;
        };
    }

    private static boolean itemsHaveCorrectType(final ListValue list, final Neo4jTypes.AnyType target) {
        return switch (list.itemValueRepresentation()) {
            case GEOMETRY -> target == NTGeometry;
            case ZONED_DATE_TIME -> target == NTDateTime;
            case LOCAL_DATE_TIME -> target == NTLocalDateTime;
            case DATE -> target == NTDate;
            case ZONED_TIME -> target == NTTime;
            case LOCAL_TIME -> target == NTLocalTime;
            case DURATION -> target == NTDuration;
            case UTF16_TEXT, UTF8_TEXT -> target == NTString;
            case BOOLEAN -> target == NTBoolean;
            case INT64, INT32, INT16, INT8, FLOAT64, FLOAT32 -> target == NTNumber;
            default -> false;
        };
    }
}
