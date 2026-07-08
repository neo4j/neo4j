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
package org.neo4j.values.storable;

import static java.time.Instant.ofEpochSecond;
import static java.time.LocalDateTime.ofInstant;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.LOCAL_DATE_TIME_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.DateValue.DATE_PATTERN;
import static org.neo4j.values.storable.DateValue.parseDate;
import static org.neo4j.values.storable.LocalTimeValue.TIME_PATTERN;
import static org.neo4j.values.storable.LocalTimeValue.parseTime;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

public final class LocalDateTimeValue extends TemporalValue<LocalDateTime, LocalDateTimeValue> {
    private static final long INSTANCE_SIZE = shallowSizeOfInstance(LocalDateTimeValue.class) + LOCAL_DATE_TIME_SIZE;

    public static final LocalDateTimeValue MIN_VALUE = new LocalDateTimeValue(LocalDateTime.MIN);
    public static final LocalDateTimeValue MAX_VALUE = new LocalDateTimeValue(LocalDateTime.MAX);
    public static final String CYPHER_TYPE_NAME = "LOCAL DATETIME";

    private final LocalDateTime value;
    private final long epochSecondsInUTC;

    private LocalDateTimeValue(LocalDateTime value) {
        this.value = value;
        this.epochSecondsInUTC = this.value.toEpochSecond(UTC);
    }

    // Only used in tests
    public static LocalDateTimeValue localDateTime(DateValue date, LocalTimeValue time) {
        return new LocalDateTimeValue(LocalDateTime.of(date.temporal(), time.temporal()));
    }

    // Only used in tests
    public static LocalDateTimeValue localDateTime(
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond) {
        return new LocalDateTimeValue(LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond));
    }

    public static LocalDateTimeValue localDateTime(LocalDateTime value) {
        return new LocalDateTimeValue(requireNonNull(value, "LocalDateTime"));
    }

    public static LocalDateTimeValue localDateTime(long epochSecond, long nano) {
        return new LocalDateTimeValue(localDateTimeRaw(epochSecond, nano));
    }

    public static LocalDateTime localDateTimeRaw(long epochSecond, long nano) {
        return assertValidArgument("epochSecond", () -> ofInstant(ofEpochSecond(epochSecond, nano), UTC));
    }

    public static LocalDateTimeValue parse(CharSequence text) {
        return parse(LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text);
    }

    public static LocalDateTimeValue parse(TextValue text) {
        return parse(LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text);
    }

    public static LocalDateTimeValue parsePattern(TextValue text, TextValue pattern) {
        try {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern.stringValue());
            TemporalAccessor parsed = dtf.parseBest(text.stringValue(), LocalDateTime::from, LocalDate::from);
            switch (parsed) {
                case LocalDateTime ldt -> {
                    return new LocalDateTimeValue(ldt);
                }
                case LocalDate ld -> {
                    return new LocalDateTimeValue(ld.atStartOfDay());
                }
                default -> throw new IllegalStateException("Unexpected value: " + parsed);
            }
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw TemporalParseException.mismatchedPattern(pattern.stringValue(), text.stringValue(), CYPHER_TYPE_NAME);
        }
    }

    public static LocalDateTimeValue now(Clock clock) {
        return new LocalDateTimeValue(LocalDateTime.now(clock));
    }

    public static LocalDateTimeValue now(Clock clock, String timezone) {
        return now(clock.withZone(parseZoneName(timezone)));
    }

    public static LocalDateTimeValue now(Clock clock, Supplier<ZoneId> defaultZone) {
        return now(clock.withZone(defaultZone.get()));
    }

    public static LocalDateTimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return StructureBuilder.build(builder(defaultZone), map);
    }

    public static LocalDateTimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return builder(defaultZone).selectDateTime(from);
    }

    public static LocalDateTimeValue truncate(
            TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone) {
        Pair<LocalDate, LocalTime> pair = getTruncatedDateAndTime(unit, input, "local date time", "LOCAL DATETIME");

        LocalDate truncatedDate = pair.first();
        LocalTime truncatedTime = pair.other();

        LocalDateTime truncatedLDT = LocalDateTime.of(truncatedDate, truncatedTime);

        if (fields.isEmpty()) {
            return localDateTime(truncatedLDT);
        } else {
            return updateFieldMapWithConflictingSubseconds(fields, unit, truncatedLDT, (mapValue, localDateTime) -> {
                if (mapValue.isEmpty()) {
                    return localDateTime(localDateTime);
                } else {
                    return build(mapValue.updatedWith("datetime", localDateTime(localDateTime)), defaultZone);
                }
            });
        }
    }

    private static final LocalDateTime DEFAULT_LOCAL_DATE_TIME = LocalDateTime.of(
            TemporalFields.year.defaultValue,
            TemporalFields.month.defaultValue,
            TemporalFields.day.defaultValue,
            TemporalFields.hour.defaultValue,
            TemporalFields.minute.defaultValue);

    private static DateTimeValue.DateTimeBuilder<LocalDateTimeValue> builder(Supplier<ZoneId> defaultZone) {
        return new DateTimeValue.DateTimeBuilder<>(defaultZone, "LOCAL DATETIME") {
            @Override
            protected boolean supportsTimeZone() {
                return false;
            }

            @Override
            protected boolean supportsEpoch() {
                return false;
            }

            @Override
            public LocalDateTimeValue buildInternal() {
                boolean selectingDate = fields.containsKey(TemporalFields.date);
                boolean selectingTime = fields.containsKey(TemporalFields.time);
                boolean selectingDateTime = fields.containsKey(TemporalFields.datetime);
                LocalDateTime result;
                if (selectingDateTime) {
                    AnyValue dtField = fields.get(TemporalFields.datetime);
                    if (!(dtField instanceof TemporalValue dt)) {
                        String prettyVal = dtField instanceof Value v ? v.prettyPrint() : String.valueOf(dtField);
                        throw InvalidArgumentException.cannotConstructTemporal(
                                "local date time", String.valueOf(dtField), prettyVal);
                    }
                    result = LocalDateTime.of(dt.getDatePart(), dt.getLocalTimePart());
                } else if (selectingTime || selectingDate) {
                    LocalTime time;
                    if (selectingTime) {
                        AnyValue timeField = fields.get(TemporalFields.time);
                        if (!(timeField instanceof TemporalValue t)) {
                            String prettyVal =
                                    timeField instanceof Value v ? v.prettyPrint() : String.valueOf(timeField);
                            throw InvalidArgumentException.cannotConstructTemporal(
                                    "local time", String.valueOf(timeField), prettyVal);
                        }
                        time = t.getLocalTimePart();
                    } else {
                        time = LocalTimeValue.DEFAULT_LOCAL_TIME;
                    }
                    LocalDate date;
                    if (selectingDate) {
                        AnyValue dateField = fields.get(TemporalFields.date);
                        if (!(dateField instanceof TemporalValue t)) {
                            String prettyVal =
                                    dateField instanceof Value v ? v.prettyPrint() : String.valueOf(dateField);
                            throw InvalidArgumentException.cannotConstructTemporal(
                                    "date", String.valueOf(dateField), prettyVal);
                        }
                        date = t.getDatePart();
                    } else {
                        date = DateValue.DEFAULT_CALENDER_DATE;
                    }

                    result = LocalDateTime.of(date, time);
                } else {
                    result = DEFAULT_LOCAL_DATE_TIME;
                }

                if (fields.containsKey(TemporalFields.week) && !selectingDate && !selectingDateTime) {
                    // Be sure to be in the start of the week based year (which can be later than 1st Jan)
                    var tempResult = result;
                    result = assertValidArgument(
                            "year",
                            () -> tempResult
                                    .with(
                                            IsoFields.WEEK_BASED_YEAR,
                                            safeCastAssignableIntegral(
                                                    TemporalFields.year.name(),
                                                    fields.get(TemporalFields.year),
                                                    TemporalFields.year.defaultValue))
                                    .with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1)
                                    .with(ChronoField.DAY_OF_WEEK, 1));
                }

                result = assignAllFields(result);
                return localDateTime(result);
            }

            private LocalDateTime getLocalDateTimeOf(AnyValue temporal) {
                if (temporal instanceof TemporalValue v) {
                    LocalDate datePart = v.getDatePart();
                    LocalTime timePart = v.getLocalTimePart();
                    return LocalDateTime.of(datePart, timePart);
                }
                String prettyVal = temporal instanceof Value v ? v.prettyPrint() : String.valueOf(temporal);
                throw InvalidArgumentException.cannotConstructTemporal("date", String.valueOf(temporal), prettyVal);
            }

            @Override
            protected LocalDateTimeValue selectDateTime(AnyValue datetime) {
                if (datetime instanceof LocalDateTimeValue) {
                    return (LocalDateTimeValue) datetime;
                }
                return localDateTime(getLocalDateTimeOf(datetime));
            }
        };
    }

    @Override
    protected int unsafeCompareTo(Value other) {
        LocalDateTimeValue that = (LocalDateTimeValue) other;
        int cmp = Long.compare(epochSecondsInUTC, that.epochSecondsInUTC);
        if (cmp == 0) {
            cmp = value.getNano() - that.value.getNano();
        }
        return cmp;
    }

    @Override
    public String getTypeName() {
        return "LocalDateTime";
    }

    @Override
    public String getTemporalCypherTypeName() {
        return CYPHER_TYPE_NAME;
    }

    @Override
    LocalDateTime temporal() {
        return value;
    }

    @Override
    LocalDate getDatePart() {
        return value.toLocalDate();
    }

    @Override
    LocalTime getLocalTimePart() {
        return value.toLocalTime();
    }

    @Override
    OffsetTime getTimePart(Supplier<ZoneId> defaultZone) {
        ZoneOffset currentOffset = assertValidArgument(
                        "time", () -> ZonedDateTime.ofInstant(Instant.now(), defaultZone.get()))
                .getOffset();
        return OffsetTime.of(value.toLocalTime(), currentOffset);
    }

    @Override
    ZoneId getZoneId(Supplier<ZoneId> defaultZone) {
        return defaultZone.get();
    }

    @Override
    ZoneOffset getZoneOffset() {
        throw UnsupportedTemporalUnitException.cannotGetZoneOffset(String.valueOf(this));
    }

    @Override
    public boolean supportsTimeZone() {
        return false;
    }

    @Override
    boolean hasTime() {
        return true;
    }

    @Override
    public boolean equals(Value other) {
        return other instanceof LocalDateTimeValue && value.equals(((LocalDateTimeValue) other).value);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeLocalDateTime(value);
    }

    @Override
    public String prettyPrint() {
        return assertPrintable(String.valueOf(value), () -> value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.LOCAL_DATE_TIME;
    }

    @Override
    protected int computeHashToMemoize() {
        return value.toInstant(UTC).hashCode();
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapLocalDateTime(this);
    }

    @Override
    public LocalDateTimeValue add(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.plus(duration), value + " + " + duration, "+"));
    }

    @Override
    public LocalDateTimeValue sub(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.minus(duration), value + " - " + duration, "-"));
    }

    @Override
    LocalDateTimeValue replacement(LocalDateTime dateTime) {
        return dateTime == value ? this : new LocalDateTimeValue(dateTime);
    }

    @Override
    public long estimatedHeapUsage() {
        return INSTANCE_SIZE;
    }

    private static final Pattern PATTERN =
            Pattern.compile(DATE_PATTERN + "(?<time>T" + TIME_PATTERN + ")?", Pattern.CASE_INSENSITIVE);

    private static LocalDateTimeValue parse(Matcher matcher) {
        return localDateTime(LocalDateTime.of(parseDate(matcher), optTime(matcher)));
    }

    static LocalTime optTime(Matcher matcher) {
        return matcher.group("time") != null ? parseTime(matcher) : LocalTime.MIN;
    }
}
