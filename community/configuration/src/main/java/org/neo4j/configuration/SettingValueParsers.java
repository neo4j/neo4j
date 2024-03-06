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

import static java.lang.Character.isDigit;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.FileUtils.fixSeparatorsInPath;
import static org.neo4j.util.Preconditions.checkArgument;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.string.SecureString;
import org.neo4j.values.storable.DateTimeValue;

public final class SettingValueParsers {
    private SettingValueParsers() {}

    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String LIST_SEPARATOR = ",";
    public static final String LIST_SEPARATOR_DESCRIPTION = "comma";

    // Pre defined parses
    public static final SettingValueParser<String> STRING = new SettingValueParser<>() {
        @Override
        public String parse(String value) {
            return value.trim();
        }

        @Override
        public String getDescription() {
            return "a string";
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    };

    public static final SettingValueParser<SecureString> SECURE_STRING = new SettingValueParser<>() {
        @Override
        public SecureString parse(String value) {
            return new SecureString(value.trim());
        }

        @Override
        public String getDescription() {
            return "a secure string";
        }

        @Override
        public Class<SecureString> getType() {
            return SecureString.class;
        }
    };

    public static final SettingValueParser<IPAddressString> CIDR_IP = new SettingValueParser<>() {
        @Override
        public IPAddressString parse(String value) {
            IPAddressString ipAddress = new IPAddressString(value.trim());
            try {
                ipAddress.validate();
            } catch (AddressStringException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid CIDR ip", value), e);
            }
            return ipAddress;
        }

        @Override
        public String getDescription() {
            return "an ip with subnet in CIDR format. e.g. 127.168.0.1/8";
        }

        @Override
        public Class<IPAddressString> getType() {
            return IPAddressString.class;
        }
    };

    public static final SettingValueParser<Byte> BYTE = new SettingValueParser<>() {
        @Override
        public Byte parse(String value) {
            try {
                return Byte.parseByte(value.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid byte value", value), e);
            }
        }

        @Override
        public Class<Byte> getType() {
            return Byte.class;
        }

        @Override
        public String getDescription() {
            return "a byte";
        }
    };

    public static final SettingValueParser<Integer> INT = new SettingValueParser<>() {
        @Override
        public Integer parse(String value) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid integer value", value), e);
            }
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }

        @Override
        public String getDescription() {
            return "an integer";
        }
    };

    public static final SettingValueParser<Long> LONG = new SettingValueParser<>() {
        @Override
        public Long parse(String value) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid long value", value), e);
            }
        }

        @Override
        public String getDescription() {
            return "a long";
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    };

    public static final SettingValueParser<Boolean> BOOL = new SettingValueParser<>() {
        @Override
        public Boolean parse(String value) {
            String trimmedValue = value.trim();
            if (trimmedValue.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (trimmedValue.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else {
                throw new IllegalArgumentException(
                        format("'%s' is not a valid boolean value, must be 'true' or 'false'", value));
            }
        }

        @Override
        public String getDescription() {
            return "a boolean";
        }

        @Override
        public Class<Boolean> getType() {
            return Boolean.class;
        }
    };

    public static final SettingValueParser<Double> DOUBLE = new SettingValueParser<>() {
        @Override
        public Double parse(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid double value", value), e);
            }
        }

        @Override
        public String getDescription() {
            return "a double";
        }

        @Override
        public Class<Double> getType() {
            return Double.class;
        }
    };

    public static final SettingValueParser<String> JVM_ADDITIONAL = new SettingValueParser<>() {
        private String parseLine(String line) {
            List<String> tokens = new ArrayList<>();
            StringBuilder sb = new StringBuilder();

            char inQuote = 0;
            for (char c : line.toCharArray()) {
                if (c == '"' || c == '\'') {
                    if (inQuote == 0) {
                        inQuote = c; // Starting new quote
                    } else if (c == inQuote) {
                        inQuote = 0; // End of current quote
                    }
                }
                if (inQuote == 0 && Character.isWhitespace(c)) {
                    addToken(tokens, sb);
                    continue;
                }
                sb.append(c);
            }
            addToken(tokens, sb);

            if (inQuote != 0) {
                throw new IllegalArgumentException("Missing end quote: " + inQuote);
            }

            return tokens.stream().map(s -> peelQuotes(s)).collect(Collectors.joining(System.lineSeparator()));
        }

        private static void addToken(List<String> tokens, StringBuilder sb) {
            if (sb.length() > 0) {
                tokens.add(sb.toString());
                sb.setLength(0);
            }
        }

        /**
         * Remove matching surrounding double and single quotes, ignoring white space characters.
         */
        private static String peelQuotes(String s) {
            s = s.strip();
            while (s.length() > 2 && (s.startsWith("'") && s.endsWith("'") || s.startsWith("\"") && s.endsWith("\""))) {
                s = s.substring(1, s.length() - 1).trim();
            }
            return s;
        }

        @Override
        public String parse(String joinedSettings) {
            // The input string already contains newline separated JVM settings. But when
            // Neo4j is running containerized all JVM settings are passed as a single environment
            // variable and in that case the JVM settings are split by space so do additional
            // parsing+splitting per line.
            //
            // Example:
            //
            // -XX:+AlwaysPreTouch
            // -DsomeValue -DsomeOther
            // "-DsomethingWithSpace=""a value"""
            // "-DquotedJustInCase" -DNotQuoted
            //
            // Should result in
            //
            // -XX:+AlwaysPreTouch
            // -DsomeValue
            // -DsomeOther
            // -DsomethingWithSpace="a value"
            // -DquotedJustInCase
            // -DNotQuoted
            //
            String[] settings = joinedSettings.split(System.lineSeparator());
            var builder = new StringBuilder();
            for (int i = 0; i < settings.length; i++) {
                if (i > 0) {
                    builder.append(System.lineSeparator());
                }
                builder.append(parseLine(settings[i].trim()));
            }
            return builder.toString();
        }

        @Override
        public String getDescription() {
            return "one or more jvm arguments";
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    };

    public static <T> SettingValueParser<List<T>> listOf(SettingValueParser<T> parser) {
        return new CollectionValueParser(List.class, Collectors.toList(), parser);
    }

    public static <T> SettingValueParser<Set<T>> setOf(SettingValueParser<T> parser) {
        return new CollectionValueParser(Set.class, Collectors.toSet(), parser);
    }

    /**
     * Base class delegating to another parser for creating collections of settings.
     *
     * @param <CT> The type of the collection.
     * @param <T>  The type of the actual element in the collection.
     */
    private static class CollectionValueParser<CT extends Collection<T>, T> implements SettingValueParser<CT> {
        private final Class<CT> collectionClass;
        private final Collector<T, ?, CT> collector;
        private final SettingValueParser<T> parser;

        CollectionValueParser(Class<CT> collectionClass, Collector<T, ?, CT> collector, SettingValueParser<T> parser) {
            this.collectionClass = collectionClass;
            this.collector = collector;
            this.parser = parser;
        }

        @Override
        public CT parse(String value) {
            return Arrays.stream(value.split(LIST_SEPARATOR))
                    .map(String::trim)
                    .filter(StringUtils::isNotEmpty)
                    .map(parser::parse)
                    .collect(collector);
        }

        @Override
        public Class<CT> getType() {
            return collectionClass;
        }

        @Override
        public String valueToString(CT value) {
            return StringUtils.join(value, LIST_SEPARATOR);
        }

        @Override
        public String getDescription() {
            return format(
                    "a %s-separated %s where each element is %s",
                    LIST_SEPARATOR_DESCRIPTION,
                    collectionClass.getSimpleName().toLowerCase(Locale.ENGLISH),
                    parser.getDescription());
        }

        @Override
        public void validate(CT value) {
            for (T element : value) {
                parser.validate(element);
            }
        }

        @Override
        public String constraintConjunction() {
            return ", which ";
        }
    }

    public static <T extends Enum<T>> SettingValueParser<T> ofEnum(final Class<T> enumClass) {
        return internalEnum(EnumSet.allOf(enumClass));
    }

    public static <T extends Enum<T>> SettingValueParser<Set<T>> setOfEnums(Class<T> enumClass) {
        SettingValueParser<T> delegate = ofEnum(enumClass);
        return new CollectionValueParser(Set.class, Collectors.toCollection(() -> EnumSet.noneOf(enumClass)), delegate);
    }

    /** An ENUM parser accepting a subset of the ENUM values
     *
     * @param values a subset of valid ENUM values to be accepted.
     * @param <T> concrete type of the enum.
     * @return a SettingValueParser parsing only provided values.
     */
    @SafeVarargs
    public static <T extends Enum<T>> SettingValueParser<T> ofPartialEnum(T... values) {
        return internalEnum(Arrays.asList(values));
    }

    private static <T extends Enum<T>> SettingValueParser<T> internalEnum(final Collection<T> values) {
        return new SettingValueParser<>() {
            @SuppressWarnings("unchecked")
            private final Class<T> type = (Class) Enum.class; // should never be empty

            @Override
            public T parse(String value) {
                String trimmedValue = value.trim();
                for (T t : values) {
                    if (t.toString().equalsIgnoreCase(trimmedValue)) {
                        return t;
                    }
                }

                throw new IllegalArgumentException(format("'%s' not one of %s", value, values));
            }

            @Override
            public void validate(T value) {
                if (!values.contains(value)) {
                    throw new IllegalArgumentException(format("'%s' not one of %s", value, values));
                }
            }

            @Override
            public String getDescription() {
                return "one of " + values;
            }

            @Override
            public Class<T> getType() {
                return type;
            }
        };
    }

    public static final SettingValueParser<HostnamePort> HOSTNAME_PORT = new SettingValueParser<>() {
        @Override
        public HostnamePort parse(String value) {
            return new HostnamePort(value);
        }

        @Override
        public String getDescription() {
            return "a hostname and port";
        }

        @Override
        public Class<HostnamePort> getType() {
            return HostnamePort.class;
        }
    };

    public static final SettingValueParser<Duration> DURATION = new SettingValueParser<>() {
        @Override
        public Duration parse(String value) {
            return Duration.ofMillis(TimeUtil.parseTimeMillis.apply(value.trim()));
        }

        @Override
        public String getDescription() {
            return "a duration (" + TimeUtil.VALID_TIME_DESCRIPTION + ")";
        }

        @Override
        public Class<Duration> getType() {
            return Duration.class;
        }

        @Override
        public String valueToString(Duration value) {
            return Duration.ZERO.equals(value) ? "0s" : TimeUtil.nanosToString(value.toNanos());
        }
    };

    public static final SettingValueParser<DurationRange> DURATION_RANGE = new SettingValueParser<>() {
        @Override
        public DurationRange parse(String value) {
            return DurationRange.parse(value);
        }

        @Override
        public String getDescription() {
            return "a duration-range <min-max> (" + TimeUtil.VALID_TIME_DESCRIPTION + ")";
        }

        @Override
        public Class<DurationRange> getType() {
            return DurationRange.class;
        }

        @Override
        public String valueToString(DurationRange value) {
            return value.valueToString();
        }
    };

    public static final SettingValueParser<ZoneId> TIMEZONE = new SettingValueParser<>() {

        @Override
        public ZoneId parse(String value) {
            try {
                return DateTimeValue.parseZoneOffsetOrZoneName(value.trim());
            } catch (Exception e) {
                throw new IllegalArgumentException(format("'%s' is not a valid timezone value", value), e);
            }
        }

        @Override
        public String getDescription() {
            return "a string describing a timezone, either described by offset (e.g. `+02:00`) or by name (e.g. `Europe/Stockholm`)";
        }

        @Override
        public Class<ZoneId> getType() {
            return ZoneId.class;
        }
    };

    public static final SettingValueParser<SocketAddress> SOCKET_ADDRESS = new SettingValueParser<>() {
        @Override
        public SocketAddress parse(String value) {
            return SocketAddressParser.socketAddress(value, SocketAddress::new);
        }

        @Override
        public String getDescription() {
            return "a socket address in the format of `hostname:port`, `hostname`, or `:port`";
        }

        @Override
        public Class<SocketAddress> getType() {
            return SocketAddress.class;
        }

        @Override
        public SocketAddress solveDependency(SocketAddress value, SocketAddress dependencyValue) {
            return solve(value, dependencyValue);
        }

        @Override
        public SocketAddress solveDefault(SocketAddress value, SocketAddress defaultValue) {
            return value != null ? solve(value, defaultValue) : null;
        }

        @Override
        public String getSolverDescription() {
            return "If missing, it is acquired";
        }

        private SocketAddress solve(SocketAddress value, SocketAddress dependencyValue) {
            if (value == null) {
                return dependencyValue;
            }
            String hostname = value.getHostname();
            int port = value.getPort();
            if (dependencyValue != null) {
                if (StringUtils.isEmpty(hostname)) {
                    hostname = dependencyValue.getHostname();
                }
                if (port < 0) {
                    port = dependencyValue.getPort();
                }
            }

            return new SocketAddress(hostname, port);
        }
    };

    public static final SettingValueParser<Long> BYTES = new SettingValueParser<>() {
        @Override
        public Long parse(String value) {
            long bytes = ByteUnit.parse(value);
            validate(bytes);
            return bytes;
        }

        @Override
        public void validate(Long value) {
            if (value < 0) {
                throw new IllegalArgumentException(
                        format("'%s' is not a valid number of bytes. Must be positive or zero.", value));
            }
        }

        @Override
        public String getDescription() {
            return format("a byte size (valid multipliers are %s)", ByteUnit.VALID_MULTIPLIERS);
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }

        @Override
        public String valueToString(Long value) {
            return ByteUnit.bytesToStringWithoutScientificNotation(value);
        }
    };

    public static final SettingValueParser<URI> URI = new SettingValueParser<>() {
        @Override
        public URI parse(String value) {
            try {
                return new URI(value);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid URI", value));
            }
        }

        @Override
        public String getDescription() {
            return "a URI";
        }

        @Override
        public Class<java.net.URI> getType() {
            return URI.class;
        }
    };

    public static SettingValueParser<URI> HTTPS_URI(boolean allowHTTPonLocalhost) {
        return new SettingValueParser<>() {
            @Override
            public java.net.URI parse(String value) {
                java.net.URI uri = URI.parse(value);
                var localhostValues = List.of("localhost", "127.0.0.1", "[::1]", "[0:0:0:0:0:0:0:1]");
                var isLocalhost = uri.getHost() != null && localhostValues.contains(uri.getHost());
                var isHttps = uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("https");

                if (isHttps || allowHTTPonLocalhost && isLocalhost) {
                    return uri;
                } else {
                    throw new IllegalArgumentException(format("'%s' does not have required scheme 'https'", value));
                }
            }

            @Override
            public String getDescription() {
                var localhostAddendum = allowHTTPonLocalhost ? " or a http URI for localhost" : "";
                return "a URI with the https:// schema" + localhostAddendum;
            }

            @Override
            public Class<java.net.URI> getType() {
                return URI.getType();
            }
        };
    }

    public static final SettingValueParser<URI> NORMALIZED_RELATIVE_URI = new SettingValueParser<>() {
        @Override
        public URI parse(String value) {
            try {
                String normalizedUri = new URI(value).normalize().getPath();
                if (normalizedUri.endsWith("/")) {
                    // Force the string end without "/"
                    normalizedUri = normalizedUri.substring(0, normalizedUri.length() - 1);
                }
                return new URI(normalizedUri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(format("'%s' is not a valid URI", value));
            }
        }

        @Override
        public String getDescription() {
            return "a normalized relative URI";
        }

        @Override
        public Class<URI> getType() {
            return URI.class;
        }
    };

    public static final SettingValueParser<Path> PATH = new SettingValueParser<>() {
        @Override
        public Path parse(String value) {
            return Path.of(fixSeparatorsInPath(StringEscapeUtils.escapeJava(value)))
                    .normalize();
        }

        @Override
        public String getDescription() {
            return "a path";
        }

        @Override
        public Class<Path> getType() {
            return Path.class;
        }

        @Override
        public Path solveDependency(Path value, Path dependencyValue) {
            requireNonNull(dependencyValue, "Dependency can not be null");
            checkArgument(dependencyValue.isAbsolute(), "Dependency must be absolute path");

            if (value != null) {
                return value.isAbsolute() ? value : dependencyValue.resolve(value);
            }
            return dependencyValue;
        }

        @Override
        public void validate(Path value) {
            if (!value.isAbsolute()) {
                throw new IllegalArgumentException(format("'%s' is not an absolute path.", value));
            }
            if (!value.equals(value.normalize())) {
                throw new IllegalArgumentException(format("'%s' is not a normalized path.", value));
            }
        }

        @Override
        public String getSolverDescription() {
            return "If relative, it is resolved";
        }
    };

    public static final SettingValueParser<String> DATABASENAME = new SettingValueParser<>() {
        @Override
        public String parse(String name) {
            validate(name);
            return name;
        }

        @Override
        public void validate(String value) {
            DatabaseNameValidator.validateExternalDatabaseName(new NormalizedDatabaseName(value));
        }

        @Override
        public String getDescription() {
            return "a valid database name containing only alphabetic characters, numbers, dots, and dashes "
                    + "with a length between "
                    + DatabaseNameValidator.MINIMUM_DATABASE_NAME_LENGTH + " and "
                    + DatabaseNameValidator.MAXIMUM_DATABASE_NAME_LENGTH
                    + " characters, " + "starting with an alphabetic character but not with the name `system`";
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    };

    public static final SettingValueParser<GlobbingPattern> GLOBBING_PATTERN = new SettingValueParser<>() {
        @Override
        public GlobbingPattern parse(String value) {
            return new GlobbingPattern(value);
        }

        @Override
        public String getDescription() {
            return "A simple globbing pattern that can use `*` and `?`.";
        }

        @Override
        public Class<GlobbingPattern> getType() {
            return GlobbingPattern.class;
        }
    };

    public static class MapPattern implements SettingValueParser<Map<String, String>> {
        private final Set<String> requiredKeys;
        private final Set<String> validKeys;

        public MapPattern(Set<String> requiredKeys, Set<String> validKeys) {
            this.requiredKeys = requiredKeys;
            this.validKeys = validKeys;
        }

        public MapPattern(Set<String> requiredKeys) {
            this(requiredKeys, null);
        }

        MapPattern() {
            this(null, null);
        }

        @Override
        public Map<String, String> parse(String map) {
            String[] splitString = map.split(";");
            var settingMap = new HashMap<String, String>();
            Arrays.stream(splitString).forEach(entry -> {
                var keyValueSplit = entry.split("=", 2);
                if (keyValueSplit.length != 2) {
                    throw new IllegalArgumentException(format("'%s' map element does not follow k1=v1 format.", entry));
                }
                String key = keyValueSplit[0];
                String value = keyValueSplit[1];

                if (validKeys != null && !validKeys.contains(key)) {
                    throw new IllegalArgumentException(
                            format("map element with key %s is not one of the accepted elements %s", key, validKeys));
                }

                settingMap.put(key, value);
            });

            if (requiredKeys != null && !settingMap.keySet().containsAll(requiredKeys)) {
                throw new IllegalArgumentException(
                        format("'%s' map does not contain all of the required keys: %s.", map, requiredKeys));
            }

            return settingMap;
        }

        @Override
        public String getDescription() {
            String description = "A simple key value map pattern `k1=v1;k2=v2`.";
            if (requiredKeys != null) {
                description += format(" Required key options are: `%s`.", requiredKeys);
            }
            if (validKeys != null) {
                description += format(" Valid key options are: `%s`.", validKeys);
            }
            return description;
        }

        @Override
        public Class<Map<String, String>> getType() {
            return (Class<Map<String, String>>) (Class) Map.class;
        }
    }

    public static final SettingValueParser<Map<String, String>> MAP_PATTERN = new MapPattern();

    public static long parseLongWithUnit(String numberWithPotentialUnit) {
        int firstNonDigitIndex = findFirstNonDigit(numberWithPotentialUnit);
        String number = numberWithPotentialUnit.substring(0, firstNonDigitIndex);

        long multiplier = 1;
        if (firstNonDigitIndex < numberWithPotentialUnit.length()) {
            String unit = numberWithPotentialUnit.substring(firstNonDigitIndex);
            if (unit.equalsIgnoreCase("k")) {
                multiplier = 1024;
            } else if (unit.equalsIgnoreCase("m")) {
                multiplier = 1024 * 1024;
            } else if (unit.equalsIgnoreCase("g")) {
                multiplier = 1024 * 1024 * 1024;
            } else {
                throw new IllegalArgumentException(
                        "Illegal unit '" + unit + "' for number '" + numberWithPotentialUnit + "'");
            }
        }

        return parseLong(number) * multiplier;
    }

    /**
     * @return index of first non-digit character in {@code numberWithPotentialUnit}. If all digits then
     * {@code numberWithPotentialUnit.length()} is returned.
     */
    private static int findFirstNonDigit(String numberWithPotentialUnit) {
        int firstNonDigitIndex = numberWithPotentialUnit.length();
        for (int i = 0; i < numberWithPotentialUnit.length(); i++) {
            if (!isDigit(numberWithPotentialUnit.charAt(i))) {
                firstNonDigitIndex = i;
                break;
            }
        }
        return firstNonDigitIndex;
    }
}
