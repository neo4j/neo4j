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
package org.neo4j.procedure.impl;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntBoolean;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntFloat;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntInteger;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTByteArray;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTTime;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.cypher.internal.evaluator.Evaluator;
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.Name;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class TypeCheckers {
    private static final ExpressionEvaluator EVALUATOR = Evaluator.expressionEvaluator();

    private static final Function<String, DefaultParameterValue> PARSE_STRING = DefaultParameterValue::ntString;
    private static final Function<String, DefaultParameterValue> PARSE_INTEGER = s -> ntInteger(parseLong(s));
    private static final Function<String, DefaultParameterValue> PARSE_FLOAT = s -> ntFloat(parseDouble(s));
    private static final Function<String, DefaultParameterValue> PARSE_NUMBER =
            new CompositeConverter(NTNumber, PARSE_INTEGER, PARSE_FLOAT);
    private static final Function<String, DefaultParameterValue> PARSE_BOOLEAN = s -> ntBoolean(parseBooleanOrFail(s));
    private static final MapConverter PARSE_MAP = new MapConverter(EVALUATOR);
    private static final ListConverter PARSE_LIST = new ListConverter(Object.class, NTAny, EVALUATOR);
    private static final ByteArrayConverter PARSE_BYTE_ARRAY = new ByteArrayConverter(EVALUATOR);

    private static final CompositeConverter PARSE_ANY = new CompositeConverter(
            NTAny,
            DefaultValueConverter.nullParser(NTAny),
            PARSE_MAP,
            PARSE_LIST,
            PARSE_BOOLEAN,
            PARSE_NUMBER,
            PARSE_STRING);
    private static final DefaultValueConverter TO_ANY = new DefaultValueConverter(NTAny, PARSE_ANY);
    private static final DefaultValueConverter TO_STRING = new DefaultValueConverter(NTString, PARSE_STRING);
    private static final DefaultValueConverter TO_INTEGER = new DefaultValueConverter(NTInteger, PARSE_INTEGER);
    private static final DefaultValueConverter TO_FLOAT = new DefaultValueConverter(NTFloat, PARSE_FLOAT);
    private static final DefaultValueConverter TO_NUMBER = new DefaultValueConverter(NTNumber, PARSE_NUMBER);
    private static final DefaultValueConverter TO_BOOLEAN = new DefaultValueConverter(NTBoolean, PARSE_BOOLEAN);
    private static final DefaultValueConverter TO_MAP = new DefaultValueConverter(NTMap, PARSE_MAP);
    private static final DefaultValueConverter TO_LIST = toList(TO_ANY, Object.class);
    private final DefaultValueConverter TO_BYTE_ARRAY = new DefaultValueConverter(NTByteArray, PARSE_BYTE_ARRAY);

    private final Map<Type, DefaultValueConverter> javaToNeo = new HashMap<>();

    public TypeCheckers() {
        super();
        registerScalarsAndCollections();
    }

    /**
     * We don't have Node, Relationship, Property available down here - and don't strictly want to, we want the procedures to be independent of which Graph API
     * is being used (and we don't want them to get tangled up with kernel code). So, we only register the "core" type system here, scalars and collection
     * types. Node, Relationship, Path and any other future graph types should be registered from the outside in the same place APIs to work with those types is
     * registered.
     */
    private void registerScalarsAndCollections() {
        registerType(String.class, TO_STRING);
        registerType(TextValue.class, TO_STRING);
        registerType(long.class, TO_INTEGER);
        registerType(Long.class, TO_INTEGER);
        registerType(IntegralValue.class, TO_INTEGER);
        registerType(double.class, TO_FLOAT);
        registerType(Double.class, TO_FLOAT);
        registerType(FloatingPointValue.class, TO_FLOAT);
        registerType(Number.class, TO_NUMBER);
        registerType(NumberValue.class, TO_NUMBER);
        registerType(boolean.class, TO_BOOLEAN);
        registerType(Boolean.class, TO_BOOLEAN);
        registerType(BooleanValue.class, TO_BOOLEAN);
        registerType(Map.class, TO_MAP);
        registerType(MapValue.class, TO_MAP);
        registerType(List.class, TO_LIST);
        registerType(ListValue.class, TO_LIST);
        registerType(Object.class, TO_ANY);
        registerType(AnyValue.class, TO_ANY);
        registerType(byte[].class, TO_BYTE_ARRAY);
        registerType(ByteArray.class, TO_BYTE_ARRAY);
        registerType(ZonedDateTime.class, new DefaultValueConverter(NTDateTime));
        registerType(DateTimeValue.class, new DefaultValueConverter(NTDateTime));
        registerType(LocalDateTime.class, new DefaultValueConverter(NTLocalDateTime));
        registerType(LocalDateTimeValue.class, new DefaultValueConverter(NTLocalDateTime));
        registerType(LocalDate.class, new DefaultValueConverter(NTDate));
        registerType(DateValue.class, new DefaultValueConverter(NTDate));
        registerType(OffsetTime.class, new DefaultValueConverter(NTTime));
        registerType(TimeValue.class, new DefaultValueConverter(NTTime));
        registerType(LocalTime.class, new DefaultValueConverter(NTLocalTime));
        registerType(LocalTimeValue.class, new DefaultValueConverter(NTLocalTime));
        registerType(TemporalAmount.class, new DefaultValueConverter(NTDuration));
        registerType(DurationValue.class, new DefaultValueConverter(NTDuration));
    }

    TypeChecker checkerFor(Type javaType) throws ProcedureException {
        return converterFor(javaType);
    }

    public DefaultValueConverter converterFor(Type javaType) throws ProcedureException {
        if (isAnyValue(javaType)) {
            // For AnyValue we support subtyping
            javaType = findValidSuperClass(javaType);
        }

        DefaultValueConverter converter = javaToNeo.get(javaType);
        if (converter != null) {
            return converter;
        }

        if (javaType instanceof ParameterizedType pt) {
            Type rawType = pt.getRawType();

            if (rawType == List.class) {
                Type type = pt.getActualTypeArguments()[0];
                return toList(converterFor(type), type);
            } else if (rawType == Map.class) {
                Type type = pt.getActualTypeArguments()[0];
                if (type != String.class) {
                    throw new ProcedureException(
                            Status.Procedure.ProcedureRegistrationFailed,
                            "Maps are required to have `String` keys - but this map has `%s` keys.",
                            type.getTypeName());
                }
                return TO_MAP;
            }
        }
        throw javaToNeoMappingError(javaType);
    }

    private boolean isAnyValue(Type type) {
        return type instanceof Class<?> && AnyValue.class.isAssignableFrom((Class<?>) type);
    }

    private Type findValidSuperClass(Type type) {
        if (type instanceof Class<?> aClass) {
            while (!javaToNeo.containsKey(aClass)) {
                aClass = aClass.getSuperclass();
            }
            return aClass;
        }
        return type;
    }

    void registerType(Class<?> javaClass, DefaultValueConverter toNeo) {
        javaToNeo.put(javaClass, toNeo);
    }

    @VisibleForTesting
    Set<Type> allTypes() {
        return javaToNeo.keySet();
    }

    private static boolean parseBooleanOrFail(String s) {
        if ("true".equalsIgnoreCase(s)) {
            return true;
        }
        if ("false".equalsIgnoreCase(s)) {
            return false;
        }
        throw new IllegalArgumentException(String.format("%s is not a valid boolean expression", s));
    }

    private static DefaultValueConverter toList(DefaultValueConverter inner, Type type) {
        return new DefaultValueConverter(NTList(inner.type()), new ListConverter(type, inner.type(), EVALUATOR));
    }

    private ProcedureException javaToNeoMappingError(Type cls) {
        List<String> types = Iterables.asList(javaToNeo.keySet()).stream()
                .filter(t -> !isAnyValue(t))
                .map(Type::getTypeName)
                .sorted(String::compareTo)
                .collect(Collectors.toList());

        return new ProcedureException(
                Status.Statement.TypeError,
                "Don't know how to map `%s` to the Neo4j Type System.%n"
                        + "Please refer to to the documentation for full details.%n"
                        + "For your reference, known types are: %s",
                cls.getTypeName(),
                types);
    }

    public abstract static class TypeChecker {
        final AnyType type;

        private TypeChecker(AnyType type) {
            this.type = type;
        }

        public AnyType type() {
            return type;
        }
    }

    public static final class DefaultValueConverter extends TypeChecker {
        private final Function<String, DefaultParameterValue> parser;

        DefaultValueConverter(AnyType type) {
            this(type, nullParser(type));
        }

        private DefaultValueConverter(AnyType type, Function<String, DefaultParameterValue> parser) {
            super(type);
            this.parser = parser;
        }

        public Optional<DefaultParameterValue> defaultValue(String defaultValue) throws ProcedureException {
            if (defaultValue.equals(Name.DEFAULT_VALUE)) {
                return Optional.empty();
            } else {
                try {
                    return Optional.of(parser.apply(defaultValue));
                } catch (Exception e) {
                    throw new ProcedureException(
                            Status.Procedure.ProcedureRegistrationFailed,
                            "Default value `%s` could not be parsed as a %s",
                            defaultValue,
                            type.toString());
                }
            }
        }

        private static Function<String, DefaultParameterValue> nullParser(Neo4jTypes.AnyType neoType) {
            return s -> {
                if (s.equalsIgnoreCase("null")) {
                    return nullValue(neoType);
                } else {
                    throw new IllegalArgumentException(
                            String.format("A %s can only have a `defaultValue = \"null\"", neoType.toString()));
                }
            };
        }
    }
}
