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
package org.neo4j.genai.util;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.commons.lang3.ClassUtils;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.lazy.LazyIterableAdapter;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.impl.Cypher5TypeCheckers;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

public final class Parameters {
    public record ParameterType(Class<?> javaType, Class<?> valueType, boolean nullable, String cypherTypeName) {
        public String cypherName() {
            return cypherTypeName + (nullable ? "" : " NOT NULL");
        }
    }

    private static final ValueMapper<Object> MAPPER = new ParameterValueMapper();

    public static class Parameter {
        private static final Cypher5TypeCheckers TYPE_CHECKERS = new Cypher5TypeCheckers();

        private final ParameterType type;
        private final Field field;
        private final Object defaultValue;

        public String name() {
            return field.getName();
        }

        public ParameterType type() {
            return type;
        }

        /**
         * The default value is the value a parameter will have if it is not explicitly
         * set in the configuration map.
         *
         * @return the default value, or null if not specified
         */
        public Object defaultValue() {
            return defaultValue;
        }

        /**
         * A parameter is considered <i>required</i> if it non-nullable and has no default value.
         *
         * @return true if the parameter is required.
         */
        public boolean isRequired() {
            return !type.nullable && defaultValue == null;
        }

        public boolean isOptional() {
            return !isRequired();
        }

        public Object getFromMap(MapValue config) {
            final var value = get(config, name());
            // not in config
            if (value == null) {
                if (isRequired()) {
                    throw new IllegalArgumentException("'%s' is expected to have been set".formatted(name()));
                }
                return defaultValue;
            }

            final var object = toJavaValueOfExpectedType(value);
            // explicitly set to null in config
            if (object == null && isRequired()) {
                throw new IllegalArgumentException("'%s' is expected to be non-null".formatted(name()));
            }
            return wrapIfNullable(object);
        }

        private Parameter(Field field, Object defaults) {
            this.field = field;
            this.type = typeOf(field);
            this.defaultValue = computeDefaultValue(defaults);
        }

        private Object computeDefaultValue(Object defaults) {
            if (defaults == null) {
                return null;
            }

            // Check for user-defined default value.
            try {
                final var defaultValue = field.get(defaults);

                if (defaultValue != null) {
                    if (annotatedAsRequired()) {
                        if (!type.javaType.isPrimitive()) {
                            throw new IllegalStateException(
                                    "'%s' cannot have a default and be explicitly annotated '@%s'"
                                            .formatted(name(), Required.class.getSimpleName()));
                        }

                        // we cannot distinguish implicit default constructed primitives
                        // and explicitly set values of that default, such as:
                        // class Params {
                        //     long x;
                        //     long y = 0L;
                        // }
                        return null;
                    }

                    if (type.nullable) {
                        throw new IllegalStateException("'%s' cannot have a default value and have type '%s'"
                                .formatted(name(), type.javaType.getSimpleName()));
                    }

                    return defaultValue;
                }

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            if (type.nullable) {
                if (annotatedAsRequired()) {
                    throw new IllegalStateException("'%s' cannot be explicitly annotated '@%s' and have type '%s'"
                            .formatted(name(), Required.class.getSimpleName(), type.javaType.getSimpleName()));
                }

                // nullable types should default to empty
                return emptyOptional();
            }

            return null;
        }

        private boolean annotatedAsRequired() {
            return field.isAnnotationPresent(Required.class);
        }

        private Object emptyOptional() {
            if (type.javaType == Optional.class) {
                return Optional.empty();
            } else if (type.javaType == OptionalLong.class) {
                return OptionalLong.empty();
            } else if (type.javaType == OptionalDouble.class) {
                return OptionalDouble.empty();
            }
            throw new IllegalArgumentException("invalid optional type");
        }

        private Object optionalOf(Object value) {
            if (type.javaType == Optional.class) {
                return Optional.of(value);
            } else if (type.javaType == OptionalLong.class) {
                return OptionalLong.of((long) value);
            } else if (type.javaType == OptionalDouble.class) {
                return OptionalDouble.of((double) value);
            }
            throw new IllegalArgumentException("invalid optional type");
        }

        private Object optionalOfNullable(Object value) {
            return value != null ? optionalOf(value) : emptyOptional();
        }

        private static ParameterType typeOf(Field field) {
            final var javaType = field.getType();
            var valueType = javaType;
            boolean nullable = false;

            if (field.getGenericType() instanceof final ParameterizedType parameterizedType) {
                if (parameterizedType.getRawType() == Optional.class) {
                    final var typeArgs = parameterizedType.getActualTypeArguments();
                    if (typeArgs.length == 1 && typeArgs[0] instanceof final Class<?> innerType) {
                        valueType = innerType;
                        nullable = true;
                    } else {
                        throw new IllegalArgumentException("parameter has invalid optional type");
                    }
                }

            } else if (javaType == OptionalLong.class) {
                valueType = long.class;
                nullable = true;

            } else if (javaType == OptionalDouble.class) {
                valueType = double.class;
                nullable = true;
            }

            try {
                final var cypherTypeName =
                        TYPE_CHECKERS.converterFor(valueType).type().toString();
                return new ParameterType(javaType, valueType, nullable, cypherTypeName);
            } catch (ProcedureException e) {
                throw new IllegalArgumentException(
                        "Parameter '%s' is of an unsupported type: %s".formatted(field.getName(), e.getMessage()));
            }
        }

        private AnyValue get(MapValue config, String key) {
            return config.containsKey(key) ? config.get(key) : null;
        }

        private Object wrapIfNullable(Object value) {
            return type.nullable ? optionalOfNullable(value) : value;
        }

        private Object toJavaValueOfExpectedType(AnyValue value) {
            if (value == NO_VALUE) {
                return null;
            }

            final var javaValue = value.map(MAPPER);
            final var actualType = javaValue.getClass();
            final var expectedType = type.valueType;

            // TODO: float <-> int coercion?
            if (!ClassUtils.isAssignable(actualType, expectedType, true)) {
                throw new IllegalArgumentException(
                        "'%s' is expected to have been of type %s".formatted(name(), type.cypherName()));
            }

            return javaValue;
        }
    }

    public static class WithDynamic {
        private Map<String, Object> dynamic;

        /**
         * @return a map containing key-value pairs that did not match any of the declared parameters.
         */
        public Map<String, Object> dynamic() {
            return dynamic;
        }
    }

    public static <T> T parse(Class<T> parameterDeclaration, MapValue map) {
        try {
            final var instance = constructInstance(parameterDeclaration);

            // Get
            final var parameters = getParameters(parameterDeclaration, instance);
            for (var parameter : parameters) {
                final var value = parameter.getFromMap(map);
                parameter.field.set(instance, value);
            }

            // If T inherits from Parameters, set its dynamic field to include
            // the remaining key-values pairs that did not match any parameter.
            if (instance instanceof final WithDynamic withDynamic) {
                final var parameterKeys = new LazyIterableAdapter<>(parameters)
                        .collect(Parameter::name)
                        .toSet();
                final var dynamicValues = Maps.mutable.<String, Object>empty();
                map.foreach((key, value) -> {
                    if (!parameterKeys.contains(key)) {
                        dynamicValues.put(key, toOptionalJavaValue(value).orElse(null));
                    }
                });
                withDynamic.dynamic = dynamicValues.asUnmodifiable();
            }

            return instance;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<Object> toOptionalJavaValue(AnyValue value) {
        return value != NO_VALUE ? Optional.of(value.map(MAPPER)) : Optional.empty();
    }

    public static <T> List<Parameter> getParameters(Class<T> parameterDeclaration) {
        final var defaults = constructInstance(parameterDeclaration);
        return getParameters(parameterDeclaration, defaults);
    }

    private static <T> List<Parameter> getParameters(Class<T> parameterDeclaration, T defaults) {
        final var parameters = new ArrayList<Parameter>();

        for (final var field : parameterDeclaration.getDeclaredFields()) {
            if (field.canAccess(defaults)) {
                parameters.add(new Parameter(field, defaults));
            }
        }

        return parameters;
    }

    private static <T> T constructInstance(Class<T> parameterDeclaration) {
        try {
            final var constructor = parameterDeclaration.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new IllegalArgumentException("%s must have an accessible zero-argument constructor"
                    .formatted(parameterDeclaration.getCanonicalName()));
        }
    }

    /**
     * Explicitly marks a field as being required, which is needed for primitives and optionals.
     */
    @Target({ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Required {}
}
