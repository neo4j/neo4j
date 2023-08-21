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
package org.neo4j.internal.helpers;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.Format.DATE_FORMAT;
import static org.neo4j.internal.helpers.Format.DEFAULT_TIME_ZONE;
import static org.neo4j.internal.helpers.Format.TIME_FORMAT;
import static org.neo4j.internal.helpers.Format.date;
import static org.neo4j.internal.helpers.Format.duration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

class FormatTest {
    @Test
    void shouldDisplayPlainCount() {
        // when
        String format = Format.count(10);

        // then
        assertTrue(format.startsWith("10"));
    }

    @Test
    void shouldDisplayThousandCount() {
        // when
        String format = Format.count(2_000);

        // then
        assertTrue(format.startsWith("2"));
        assertTrue(format.endsWith("k"));
    }

    @Test
    void shouldDisplayMillionCount() {
        // when
        String format = Format.count(2_000_000);

        // then
        assertTrue(format.startsWith("2"));
        assertTrue(format.endsWith("M"));
    }

    @Test
    void shouldDisplayBillionCount() {
        // when
        String format = Format.count(2_000_000_000);

        // then
        assertTrue(format.startsWith("2"));
        assertTrue(format.endsWith("G"));
    }

    @Test
    void shouldDisplayTrillionCount() {
        // when
        String format = Format.count(4_000_000_000_000L);

        // then
        assertTrue(format.startsWith("4"));
        assertTrue(format.endsWith("T"));
    }

    @Test
    void displayDuration() {
        assertThat(duration(MINUTES.toMillis(1) + SECONDS.toMillis(2))).isEqualTo("1m 2s");
        assertThat(duration(42)).isEqualTo("42ms");
        assertThat(duration(0)).isEqualTo("0ms");
    }

    @Test
    void displayDate() throws ParseException {
        long timeWithDate = System.currentTimeMillis();
        String dateAsString = date(timeWithDate);
        assertEquals(
                timeWithDate,
                new SimpleDateFormat(DATE_FORMAT).parse(dateAsString).getTime());
        assertEquals(
                "2017-04-05 00:00:00.000+0000",
                date(LocalDate.of(2017, 4, 5).atStartOfDay(UTC).toInstant()));
    }

    @Test
    void displayTime() throws ParseException {
        long timeWithDate = System.currentTimeMillis();
        String timeAsString = Format.time(timeWithDate);
        assertEquals(
                timeWithDate,
                translateToDate(
                        timeWithDate,
                        new SimpleDateFormat(TIME_FORMAT).parse(timeAsString).getTime(),
                        TimeZone.getTimeZone(DEFAULT_TIME_ZONE)));
        assertEquals(
                "2017-04-05 06:07:08.000+0000",
                date(LocalDateTime.of(2017, 4, 5, 6, 7, 8, 9).toInstant(UTC)));
    }

    @Test
    void numberToStringWithGroups() {
        // given
        long number1 = 123_456_789;
        long number2 = 10_000;

        // when
        String number1AsString = Format.numberToStringWithGroups(number1, ',');
        String number2AsString = Format.numberToStringWithGroups(number2, '.');

        // then
        assertThat(number1AsString).isEqualTo("123,456,789");
        assertThat(number2AsString).isEqualTo("10.000");
    }

    private static long translateToDate(long timeWithDate, long time, TimeZone timeIsGivenInThisTimeZone) {
        Calendar calendar = Calendar.getInstance(timeIsGivenInThisTimeZone);
        calendar.setTimeInMillis(timeWithDate);

        Calendar timeCalendar = Calendar.getInstance();
        timeCalendar.setTimeInMillis(time);
        timeCalendar.setTimeZone(timeIsGivenInThisTimeZone);
        timeCalendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR));
        timeCalendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH));
        boolean crossedDayBoundary = !DEFAULT_TIME_ZONE.equals(timeIsGivenInThisTimeZone.toZoneId())
                && timeCalendar.get(Calendar.HOUR_OF_DAY) < calendar.get(Calendar.HOUR_OF_DAY);
        timeCalendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + (crossedDayBoundary ? 1 : 0));
        return timeCalendar.getTimeInMillis();
    }
}
