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
package org.neo4j.configuration;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public final class SettingConstraints {
    private SettingConstraints() {}

    public static SettingConstraint<String> except(String... forbiddenValues) {
        return new SettingConstraint<>() {
            @Override
            public void validate(String value, Configuration config) {
                if (StringUtils.isNotBlank(value)) {
                    if (ArrayUtils.contains(forbiddenValues, value)) {
                        throw new IllegalArgumentException(format("not allowed value is: %s", value));
                    }
                }
            }

            @Override
            public String getDescription() {
                if (forbiddenValues.length > 1) {
                    return format("is none of %s", Arrays.toString(forbiddenValues));
                } else if (forbiddenValues.length == 1) {
                    return format("is not `%s`", forbiddenValues[0]);
                }
                return "";
            }
        };
    }

    public static SettingConstraint<String> matches(String regex, String description) {
        return new SettingConstraint<>() {
            private final String descMsg = StringUtils.isEmpty(description) ? "" : format(" (%s)", description);
            private final Pattern pattern = Pattern.compile(regex);

            @Override
            public void validate(String value, Configuration config) {
                if (!pattern.matcher(value).matches()) {
                    throw new IllegalArgumentException(
                            format("value does not match expression: `%s`%s", regex, descMsg));
                }
            }

            @Override
            public String getDescription() {
                return format("matches the pattern `%s`%s", regex, descMsg);
            }
        };
    }

    public static SettingConstraint<String> matches(String regex) {
        return matches(regex, null);
    }

    public static <T extends Comparable<T>> SettingConstraint<T> min(final T minValue) {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {
                if (value == null) {
                    throw new IllegalArgumentException("can not be null");
                }

                if (minValue.compareTo(value) > 0) {
                    throw new IllegalArgumentException(format("minimum allowed value is %s", valueToString(minValue)));
                }
            }

            @Override
            public String getDescription() {
                return format("is minimum `%s`", valueToString(minValue));
            }
        };
    }

    public static <T extends Comparable<T>> SettingConstraint<T> max(final T maxValue) {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {
                if (value == null) {
                    throw new IllegalArgumentException("can not be null");
                }

                if (maxValue.compareTo(value) < 0) {
                    throw new IllegalArgumentException(format("maximum allowed value is %s", valueToString(maxValue)));
                }
            }

            @Override
            public String getDescription() {
                return format("is maximum `%s`", valueToString(maxValue));
            }
        };
    }

    public static <T extends Comparable<T>> SettingConstraint<T> range(final T minValue, final T maxValue) {
        return new SettingConstraint<>() {
            private final SettingConstraint<T> max = max(maxValue);
            private final SettingConstraint<T> min = min(minValue);

            @Override
            public void validate(T value, Configuration config) {
                min.validate(value, config);
                max.validate(value, config);
            }

            @Override
            public String getDescription() {
                return format("is in the range `%s` to `%s`", valueToString(minValue), valueToString(maxValue));
            }
        };
    }

    public static <T> SettingConstraint<T> is(final T expected) {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {
                if (!Objects.equals(value, expected)) {
                    throw new IllegalArgumentException(format("is not `%s`", valueToString(expected)));
                }
            }

            @Override
            public String getDescription() {
                return format("is `%s`", valueToString(expected));
            }
        };
    }

    @SafeVarargs
    public static <T> SettingConstraint<T> any(SettingConstraint<T> first, SettingConstraint<T>... rest) {
        return new SettingConstraint<>() {
            private final SettingConstraint<T>[] constraints = ArrayUtil.concat(first, rest);

            @Override
            public void validate(T value, Configuration config) {
                for (SettingConstraint<T> constraint : constraints) {
                    try {
                        constraint.validate(value, config);
                        return; // Only one constraint needs to pass for this to pass.
                    } catch (RuntimeException e) {
                        // Ignore
                    }
                }
                throw new IllegalArgumentException(format("does not fulfill any of %s", getDescription()));
            }

            @Override
            public String getDescription() {
                return stream(constraints)
                        .map(SettingConstraint::getDescription)
                        .collect(joining(" or "));
            }

            @Override
            void setParser(SettingValueParser<T> parser) {
                super.setParser(parser);
                stream(constraints).forEach(constraint -> constraint.setParser(parser));
            }
        };
    }

    public static final SettingConstraint<Long> POWER_OF_2 = new SettingConstraint<>() {
        @Override
        public void validate(Long value, Configuration config) {
            if (value != null && !Numbers.isPowerOfTwo(value)) {
                throw new IllegalArgumentException("only power of 2 values allowed");
            }
        }

        @Override
        public String getDescription() {
            return "is power of 2";
        }
    };

    public static <T> SettingConstraint<List<T>> size(final int size) {
        return new SettingConstraint<>() {
            @Override
            public void validate(List<T> value, Configuration config) {
                if (value == null) {
                    throw new IllegalArgumentException("can not be null");
                }

                if (value.size() != size) {
                    throw new IllegalArgumentException(format("needs to be of size %s", size));
                }
            }

            @Override
            public String getDescription() {
                return format("is of size `%s`", size);
            }
        };
    }

    public static <T> SettingConstraint<List<T>> noDuplicates() {
        return new SettingConstraint<>() {
            @Override
            public void validate(List<T> value, Configuration config) {
                if (Set.copyOf(value).size() != value.size()) {
                    throw new IllegalArgumentException(
                            format("items should not have duplicates: %s", valueToString(value)));
                }
            }

            @Override
            public String getDescription() {
                return "no duplicate items";
            }
        };
    }

    public static SettingConstraint<List<String>> singleControlledValueOrFreeList(final String controlledvalue) {
        return new SettingConstraint<>() {
            @Override
            public void validate(List<String> list, Configuration config) {
                if (list != null && list.size() > 1 && list.contains(controlledvalue)) {
                    throw new IllegalArgumentException(format(
                            "The list's length can not be greater than 1 if it contains the value %s.",
                            controlledvalue));
                }
            }

            @Override
            public String getDescription() {
                return format("One single controlled value (`%s`) or free list.", controlledvalue);
            }
        };
    }

    public static final SettingConstraint<SocketAddress> HOSTNAME_ONLY = new SettingConstraint<>() {
        @Override
        public void validate(SocketAddress value, Configuration config) {
            if (value == null) {
                throw new IllegalArgumentException("can not be null");
            }

            if (value.getPort() >= 0) {
                throw new IllegalArgumentException("can not have a port");
            }

            if (StringUtils.isBlank(value.getHostname())) {
                throw new IllegalArgumentException("needs not a hostname");
            }
        }

        @Override
        public String getDescription() {
            return "has no specified port";
        }
    };

    public static final SettingConstraint<SocketAddress> NO_ALL_INTERFACES_ADDRESS = new SettingConstraint<>() {
        @Override
        public void validate(SocketAddress value, Configuration config) {
            if (value != null && value.getHostname() != null) {
                if (value.getHostname().matches("^0+\\.0+\\.0+\\.0+$")) {
                    throw new IllegalArgumentException("advertised address cannot be '0.0.0.0'");
                }

                if ("::".equals(value.getHostname())) {
                    throw new IllegalArgumentException("advertised address cannot be '::'");
                }
            }
        }

        @Override
        public String getDescription() {
            return "accessible address";
        }
    };

    public static final SettingConstraint<String> VALID_DATABASE_NAME = new SettingConstraint<>() {
        @Override
        public void validate(String value, Configuration config) {
            DatabaseNameValidator.validateExternalDatabaseName(new NormalizedDatabaseName(value));
        }

        @Override
        public String getDescription() {
            return "must be valid database name";
        }
    };

    public static final SettingConstraint<Path> ABSOLUTE_PATH = new SettingConstraint<>() {
        @Override
        public void validate(Path value, Configuration config) {
            if (!value.isAbsolute()) {
                throw new IllegalArgumentException(format("`%s` is not an absolute path.", valueToString(value)));
            }
        }

        @Override
        public String getDescription() {
            return "is absolute";
        }
    };

    public static <T, U> SettingConstraint<T> dependency(
            SettingConstraint<T> ifConstraint,
            SettingConstraint<T> elseConstraint,
            Setting<U> dependency,
            SettingConstraint<U> condition) {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {
                U depValue = config.get(dependency);
                try {
                    condition.validate(depValue, config);
                } catch (IllegalArgumentException e) {
                    elseConstraint.validate(value, config);
                    return;
                }
                ifConstraint.validate(value, config);
            }

            @Override
            public String getDescription() {
                return format(
                        "depends on %s. If %s %s then it %s otherwise it %s.",
                        dependency.name(),
                        dependency.name(),
                        condition.getDescription(),
                        ifConstraint.getDescription(),
                        elseConstraint.getDescription());
            }

            @Override
            void setParser(SettingValueParser<T> parser) {
                super.setParser(parser);
                ifConstraint.setParser(parser);
                elseConstraint.setParser(parser);
                condition.setParser(((SettingImpl<U>) dependency).parser());
            }
        };
    }

    public static <T> SettingConstraint<T> unconstrained() {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {}

            @Override
            public String getDescription() {
                return "is unconstrained";
            }
        };
    }

    public static SettingConstraint<Integer> greaterThanOrEqual(Setting<Integer> other) {
        return new SettingConstraint<>() {
            @Override
            public void validate(Integer value, Configuration config) {
                var otherValue = config.get(other);
                if (value == null) {
                    throw new IllegalArgumentException("can not be null");
                }
                if (otherValue == null) {
                    throw new IllegalArgumentException(other.name() + " can not be null");
                }
                if (value < otherValue) {
                    throw new IllegalArgumentException(getDescription()
                            + format("was %d, which is not more than or equal to %d", value, otherValue));
                }
            }

            @Override
            public String getDescription() {
                return format("Must be set greater than or equal to value of '%s'", other.name());
            }
        };
    }

    public static <T> SettingConstraint<T> lessThanOrEqual(
            Function<T, Long> converter,
            Setting<T> other,
            LongFunction<Long> otherModifier,
            String modifierDescription) {
        return new SettingConstraint<>() {
            @Override
            public void validate(T value, Configuration config) {
                var otherValue = config.get(other);
                if (value == null) {
                    throw new IllegalArgumentException("can not be null");
                }
                if (otherValue == null) {
                    throw new IllegalArgumentException(other.name() + " can not be null");
                }

                var thisAsLong = converter.apply(value);
                var otherAsLong = converter.apply(otherValue);
                if (thisAsLong == null) {
                    throw new IllegalStateException("Result of " + converter + " on " + value + " can not be null");
                }
                if (otherAsLong == null) {
                    throw new IllegalStateException(
                            "Result of " + converter + " on " + other.name() + " (" + otherValue + ") can not be null");
                }

                var modifiedOther = otherModifier.apply(otherAsLong);
                if (modifiedOther == null) {
                    throw new IllegalStateException(
                            "Result of " + otherModifier + " on " + other.name() + " (" + otherAsLong + ") was null");
                }

                if (thisAsLong > modifiedOther) {
                    throw new IllegalArgumentException(getDescription()
                            + format(
                                    ". was %d, which is not less than or equal to %d from %s",
                                    thisAsLong, modifiedOther, other.name()));
                }
            }

            @Override
            public String getDescription() {
                return format("Must be set less than or equal to value of '%s' %s", other.name(), modifierDescription);
            }
        };
    }

    public static <T> SettingConstraint<T> lessThanOrEqual(Function<T, Long> converter, Setting<T> other) {
        return lessThanOrEqual(converter, other, s -> s, "");
    }

    public static SettingConstraint<Integer> lessThanOrEqual(Setting<Integer> other) {
        return lessThanOrEqual(Long::valueOf, other);
    }

    public static SettingConstraint<Long> lessThanOrEqualLong(Setting<Long> other) {
        return lessThanOrEqual(Long::valueOf, other);
    }

    public static <T, C extends Collection<T>> SettingConstraint<C> shouldNotContain(
            T value, String collectionDescription) {
        return new SettingConstraint<>() {
            @Override
            public void validate(C coll, Configuration config) {
                if (coll.contains(value)) {
                    throw new IllegalArgumentException(getDescription());
                }
            }

            @Override
            public String getDescription() {
                return String.format("Value '%s' can't be included in %s!", value.toString(), collectionDescription);
            }
        };
    }
}
