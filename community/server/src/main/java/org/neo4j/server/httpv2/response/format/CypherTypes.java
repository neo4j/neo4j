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
package org.neo4j.server.httpv2.response.format;

import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;
import org.neo4j.values.storable.DurationValue;

/**
 * "Official" Cypher types.
 *
 * @author Michael J. Simons
 */
public enum CypherTypes {
    Null,

    List,

    Map,

    Boolean,

    Integer(java.lang.Long::parseLong, Value::toString),

    Float(Double::parseDouble, Value::toString),

    String(s -> s, Value::asString),

    Base64(v -> java.util.Base64.getDecoder().decode(v), v -> java.util.Base64.getEncoder()
            .encodeToString(v.asByteArray())),

    Date(
            v -> LocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE),
            v -> DateTimeFormatter.ISO_LOCAL_DATE.format(v.asLocalDate())),

    Time(
            v -> OffsetTime.parse(v, DateTimeFormatter.ISO_OFFSET_TIME),
            v -> DateTimeFormatter.ISO_OFFSET_TIME.format(v.asOffsetTime())),

    LocalTime(
            v -> java.time.LocalTime.parse(v, DateTimeFormatter.ISO_LOCAL_TIME),
            v -> DateTimeFormatter.ISO_LOCAL_TIME.format(v.asLocalTime())),

    DateTime(v -> java.time.ZonedDateTime.parse(v, DateTimeFormatter.ISO_ZONED_DATE_TIME), v -> {
        if (v.asZonedDateTime().getZone().normalized() instanceof ZoneOffset) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(v.asOffsetDateTime());
        } else {
            return DateTimeFormatter.ISO_ZONED_DATE_TIME.format(v.asZonedDateTime());
        }
    }),

    OffsetDateTime(
            v -> java.time.OffsetDateTime.parse(v, DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            v -> DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(v.asOffsetDateTime())),
    ZonedDateTime(
            v -> java.time.ZonedDateTime.parse(v, DateTimeFormatter.ISO_ZONED_DATE_TIME),
            v -> DateTimeFormatter.ISO_ZONED_DATE_TIME.format(v.asZonedDateTime())),

    LocalDateTime(
            v -> java.time.LocalDateTime.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME),
            v -> DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v.asLocalDateTime())),

    // note: Java Driver currently formats the Duration string in a non-standard way so we do this dance for now.
    Duration(
            CypherTypes::parseDuration,
            v -> DurationValue.parse(v.asIsoDuration().toString()).toString()),

    Point(CypherTypes::parsePoint, null),

    Node,

    Relationship,

    Path;

    private final String value;

    private final Function<java.lang.String, Value> reader;
    private final Function<Value, java.lang.String> writer;

    CypherTypes() {
        this(null, null, null);
    }

    CypherTypes(Function<String, Object> reader, Function<Value, String> writer) {
        this(null, reader, writer);
    }

    CypherTypes(String value, Function<String, Object> reader, Function<Value, String> writer) {

        this.value = value == null ? this.name() : value;
        this.reader = reader == null ? null : reader.andThen(Values::value);
        this.writer = writer;
    }

    public String getValue() {
        return value;
    }

    /**
     * {@return optional reader if this type can be read directly}
     */
    public Function<java.lang.String, Value> getReader() {
        return reader;
    }

    /**
     * {@return optional writer if this type can be written directly}
     */
    public Function<Value, java.lang.String> getWriter() {
        return writer;
    }

    private static final Pattern WKT_PATTERN =
            Pattern.compile("SRID=(\\d+);\\s*POINT\\(\\s*(\\S+)\\s+(\\S+)\\s*(\\S?)\\)");

    /**
     * Pragmatic parsing of the Neo4j Java Driver's {@link org.neo4j.driver.types.Point} class
     * This method does not check if the parameters align with the given coordinate system or if the coordinate system code is valid.
     *
     * @param input WKT representation of a point
     */
    private static Point parsePoint(String input) {
        var matcher = WKT_PATTERN.matcher(input);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Illegal pattern"); // todo add right pattern syntax in exception message
        }

        var srid = java.lang.Integer.parseInt(matcher.group(1));
        var x = Double.parseDouble(matcher.group(2));
        var y = Double.parseDouble(matcher.group(3));
        var z = matcher.group(4);
        if (z != null && !z.isBlank()) {
            return Values.point(srid, x, y, Double.parseDouble(z)).asPoint();
        } else {
            return Values.point(srid, x, y).asPoint();
        }
    }

    private static IsoDuration parseDuration(String input) {
        var d = DurationValue.parse(input);

        return new InternalIsoDuration(
                d.get(ChronoUnit.MONTHS), d.get(ChronoUnit.DAYS), d.get(ChronoUnit.SECONDS), (int)
                        d.get(ChronoUnit.NANOS));
    }
}
