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
package org.neo4j.shell.parameter;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;
import static org.neo4j.shell.TransactionHandler.TransactionType.USER_TRANSPILED;

import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.internal.literal.interpreter.LiteralInterpreter;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;

public interface ParameterService {
    /**
     * Returns all set parameters.
     */
    Map<String, org.neo4j.driver.Value> parameters();

    /**
     * Evaluate parameters.
     *
     * Simple expressions are evaluated offline,
     * but complex expressions needs an open connection for this call to succeed.
     */
    List<Parameter> evaluate(RawParameters parameter) throws CommandException;

    /** Set parameters. */
    void setParameters(List<Parameter> parameters);

    /** Parse parameters. */
    RawParameters parse(String input) throws ParameterParsingException;

    /** Returns a pretty formatted string of currently set parameters. */
    String pretty();

    /** Clear parameters. */
    void clear();

    static ParameterService create(TransactionHandler db) {
        return new ShellParameterService(db);
    }

    static ParameterParser createParser() {
        return new ShellParameterService.ShellParameterParser();
    }

    interface ParameterParser {
        RawParameters parse(String input) throws ParameterParsingException;
    }

    interface ParameterEvaluator {
        List<Parameter> evaluate(RawParameters parameter) throws CommandException;
    }

    /** Parameters represented as a Cypher map expression */
    record RawParameters(String expression) {}

    record Parameter(String name, org.neo4j.driver.Value value) {}

    class ParameterParsingException extends RuntimeException {}

    class ParameterEvaluationException extends RuntimeException {
        ParameterEvaluationException(String message, Throwable cause) {
            super(message, cause);
        }

        ParameterEvaluationException(String message) {
            super(message);
        }
    }
}

class ShellParameterService implements ParameterService {
    private static final Logger log = Logger.create();
    private final Map<String, org.neo4j.driver.Value> parameters = new HashMap<>();
    private final ParameterParser parser = new ShellParameterParser();
    private final ParameterEvaluator evaluator;
    private final ParameterPrettyRenderer prettyRenderer = ParameterPrettyRenderer.create();

    ShellParameterService(TransactionHandler db) {
        this.evaluator = new ShellParameterEvaluator(db);
    }

    @Override
    public Map<String, org.neo4j.driver.Value> parameters() {
        return parameters;
    }

    @Override
    public void setParameters(List<Parameter> parameters) {
        parameters.forEach(p -> {
            this.parameters.put(p.name(), p.value());
        });
    }

    @Override
    public List<Parameter> evaluate(RawParameters parameter) throws CommandException {
        return evaluator.evaluate(parameter);
    }

    @Override
    public RawParameters parse(String input) throws ParameterParsingException {
        return parser.parse(input);
    }

    @Override
    public String pretty() {
        return prettyRenderer.pretty(parameters());
    }

    @Override
    public void clear() {
        parameters.clear();
    }

    public static class ShellParameterParser implements ParameterParser {
        private final CypherMapParameterParser mapParser = new CypherMapParameterParser();
        private final ArrowParameterParser arrowParser = new ArrowParameterParser();

        @Override
        public RawParameters parse(String input) throws ParameterParsingException {
            return doParse(stripTrailingSemicolon(input));
        }

        private RawParameters doParse(String input) throws ParameterParsingException {
            return Optional.ofNullable(mapParser.parse(input))
                    .or(() -> Optional.ofNullable(arrowParser.parse(input)))
                    .orElseThrow(ParameterParsingException::new);
        }

        private static String stripTrailingSemicolon(String input) {
            return StringUtils.stripEnd(input.trim(), ";");
        }
    }

    private static class CypherMapParameterParser implements ParameterParser {
        private static final Pattern CYPHER_MAP_PATTERN = Pattern.compile("^\\s*\\{");

        @Override
        public RawParameters parse(String input) throws ParameterParsingException {
            if (CYPHER_MAP_PATTERN.matcher(input).find()) {
                return new RawParameters(input);
            } else {
                return null;
            }
        }
    }

    // Legacy parser
    private static class ArrowParameterParser implements ParameterParser {
        private final List<Pattern> patterns = List.of(
                Pattern.compile("^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*)\\s*=>\\s*(?<value>.+)$"),
                Pattern.compile("^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):?\\s+(?<value>.+)$"),
                Pattern.compile("^\\s*(?<key>(`([^`])*`)+?)\\s*=>\\s*(?<value>.+)$"),
                Pattern.compile("^\\s*(?<key>(`([^`])*`)+?):?\\s+(?<value>.+)$"));
        private final Pattern invalidPattern =
                Pattern.compile("^\\s*(?<key>[\\p{L}_][\\p{L}0-9_]*):\\s*=>\\s*(?<value>.+)$");

        @Override
        public RawParameters parse(String input) throws ParameterParsingException {
            if (invalidPattern.matcher(input).matches()) {
                throw new ParameterParsingException();
            }

            return patterns.stream()
                    .map(p -> p.matcher(input))
                    .filter(Matcher::matches)
                    .findFirst()
                    .filter(m -> !m.group("key").isBlank() && !m.group("key").equals("``"))
                    .map(m -> new RawParameters(String.format("{%s: %s}", m.group("key"), m.group("value"))))
                    .orElse(null);
        }
    }

    private class ShellParameterEvaluator implements ParameterEvaluator {
        private final TransactionHandler db;

        private ShellParameterEvaluator(TransactionHandler db) {
            this.db = db;
        }

        @Override
        public List<Parameter> evaluate(RawParameters parameter) throws CommandException {
            final var exp = parameter.expression();
            final var parameterMap = evaluateOffline(exp).orElseGet(() -> evaluateOnline(exp));
            return asParameters(exp, parameterMap);
        }

        private List<Parameter> asParameters(String expression, org.neo4j.driver.Value value) {
            if (value.hasType(TYPE_SYSTEM.MAP())) {
                return value.asMap(v -> v).entrySet().stream()
                        .map(e -> new Parameter(e.getKey(), e.getValue()))
                        .toList();
            } else {
                final var message = "Failed to evaluate parameters " + expression + ", got " + value;
                throw new ParameterEvaluationException(message);
            }
        }

        private Optional<org.neo4j.driver.Value> evaluateOffline(String expression) {
            try {
                return Optional.of(toDriverValue(LiteralInterpreter.parseExpression(expression)));
            } catch (Exception e) {
                log.warn("Failed to evaluate expression " + expression + " locally", e);
                return Optional.empty();
            }
        }

        /*
         * Converts JavaCC parser output to driver values.
         * JavaCC returns std lib java classes most of the time,
         * but there are some exceptions where it returns neo4j values.
         */
        private static org.neo4j.driver.Value toDriverValue(Object input) {
            if (input == null) {
                return NullValue.NULL;
            } else if (input instanceof Map<?, ?> map) {
                return new org.neo4j.driver.internal.value.MapValue(map.entrySet().stream()
                        .collect(Collectors.toMap(e -> (String) e.getKey(), e -> toDriverValue(e.getValue()))));
            } else if (input instanceof Iterable<?> iterable) {
                return org.neo4j.driver.Values.value(StreamSupport.stream(iterable.spliterator(), false)
                        .map(ShellParameterEvaluator::toDriverValue)
                        .toList());
            } else if (input instanceof org.neo4j.values.storable.DurationValue duration) {
                if (duration.getUnits().equals(List.of(MONTHS, DAYS, SECONDS, NANOS))) {
                    final var months = duration.get(MONTHS);
                    final var days = duration.get(DAYS);
                    final var seconds = duration.get(SECONDS);
                    final var nanos = Math.toIntExact(duration.get(NANOS));
                    return org.neo4j.driver.Values.isoDuration(months, days, seconds, nanos);
                } else {
                    throw new ParameterEvaluationException("Paths not supported");
                }
            } else if (input instanceof org.neo4j.values.storable.PointValue point) {
                final var srid = point.getCoordinateReferenceSystem().getCode();
                final var coords = point.getCoordinate().getCoordinate();
                if (coords.length == 2) {
                    return org.neo4j.driver.Values.point(srid, coords[0], coords[1]);
                } else if (coords.length == 3) {
                    return org.neo4j.driver.Values.point(srid, coords[0], coords[1], coords[2]);
                } else {
                    throw new ParameterParsingException();
                }
            } else if (!(input instanceof Temporal)) {
                // Temporal values are not safe to use
                // because for example default time zone can be different between client and server
                return org.neo4j.driver.Values.value(input);
            } else {
                throw new ParameterParsingException();
            }
        }

        private org.neo4j.driver.Value evaluateOnline(String expression) {
            try {
                // Feels very wrong to execute user data unescaped...
                final var query = "RETURN " + expression + " AS `result`";

                return db.runCypher(query, parameters(), USER_TRANSPILED)
                        .map(r -> r.iterate().next().get("result"))
                        .orElseThrow();
            } catch (Exception e) {
                final var message = "Failed to evaluate expression " + expression + ": " + e.getMessage();
                throw new ParameterEvaluationException(message, e);
            }
        }
    }
}
