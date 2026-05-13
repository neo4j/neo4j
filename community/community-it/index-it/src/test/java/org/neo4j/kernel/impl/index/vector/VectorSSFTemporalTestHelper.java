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
package org.neo4j.kernel.impl.index.vector;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

public class VectorSSFTemporalTestHelper {

    static List<String> sortByDateTimeZoneOffsetAndId(List<String> dates) {

        dates.sort((o1, o2) -> Values.COMPARATOR.compare(
                DateTimeValue.parse(o1, ZoneId::systemDefault), DateTimeValue.parse(o2, ZoneId::systemDefault)));
        return dates;
    }

    static List<String> generateTestZonedTimeStrings() {
        List<Object> hours = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            hours.add(String.format("%02d:00:00.000", i));
        }
        List<String> offsets = List.of(
                        "-1100", "-1000", "-0900", "-0800", "-0700", "-0600", "-0500", "-0400", "-0300", "-0200",
                        "-0100", "+0000", "+0100", "+0200", "+0300", "+0400", "+0500", "+0600", "+0700", "+0800",
                        "+0900", "+1000", "+1100")
                .reversed();
        List<String> allTimes = new ArrayList<>();
        for (Object hour : hours) {
            // Here we do not use names (timezone ids) - they are not valid for raw times
            for (String offset : offsets) {
                allTimes.add(String.format("%s%s", hour, offset));
            }
        }

        return allTimes;
    }

    /// Build a comprehensive list of DateTimeValues with zone offset and zone id
    /// this can be sorted (via `sortByDateTimeZoneOffsetAndId`)
    /// and used to validate that
    /// 1. Cypher respects the order, as we expect that it already does
    /// 2.
    static List<String> generateTestZonedDateTimeStrings() {
        List<String> dates = List.of("2019-06-01", "2019-06-02", "2019-06-03");
        List<Object> hours = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            hours.add(String.format("%02d:00:00.000", i));
        }
        List<String> offsets = List.of(
                        "-1100", "-1000", "-0900", "-0800", "-0700", "-0600", "-0500", "-0400", "-0300", "-0200",
                        "-0100", "+0000", "+0100", "+0200", "+0300", "+0400", "+0500", "+0600", "+0700", "+0800",
                        "+0900", "+1000", "+1100")
                .reversed();
        List<String> allDateTimes = new ArrayList<>();
        for (String date : dates) {
            for (Object hour : hours) {
                for (String zone : ZoneId.getAvailableZoneIds()) {
                    allDateTimes.add(String.format("%sT%s[%s]", date, hour, zone));
                }
                for (String offset : offsets) {
                    allDateTimes.add(String.format("%sT%s%s", date, hour, offset));
                }
            }
        }
        return allDateTimes;
    }

    static List<String> sortByTimeZoneOffsetAndId(List<String> times) {

        times.sort((o1, o2) -> Values.COMPARATOR.compare(
                TimeValue.parse(o1, ZoneId::systemDefault), TimeValue.parse(o2, ZoneId::systemDefault)));
        return times;
    }
}
