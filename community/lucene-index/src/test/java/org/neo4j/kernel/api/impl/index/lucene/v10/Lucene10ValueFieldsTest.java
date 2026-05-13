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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.Values;

public class Lucene10ValueFieldsTest {

    @Test
    public void temporalWithZoneOrderOffsetRegionFix() {
        checkTemporalWithZoneOrder(List.of(
                "2019-06-02T21:00:00.000[Atlantic/Cape_Verde]",
                "2019-06-02T21:00:00.000[Etc/GMT+1]",
                "2019-06-02T22:00:00.000+0000",
                "2019-06-02T22:00:00.000[Africa/Abidjan]",
                "2019-06-02T22:00:00.000[Africa/Accra]",
                "2019-06-02T22:00:00.000[Africa/Bamako]"));
    }

    @Test
    public void temporalWithZoneOrderInstant() {
        checkTemporalWithZoneOrder(
                List.of("2019-06-02T21:00:00.000[Africa/Bamako]", "2019-06-02T21:00:00.001[Africa/Abidjan]"));
    }

    @Test
    public void temporalWithZoneOrderOffset() {
        checkTemporalWithZoneOrder(List.of(
                "2019-06-02T19:00:00.000-0200",
                "2019-06-02T20:00:00.000-0100",
                "2019-06-02T21:00:00.000+0000",
                "2019-06-02T22:00:00.00+0100"));
    }

    @Test
    public void validateTemporalStates() {
        assertThatThrownBy(() -> new Lucene10ValueFields.TemporalWithZone<>(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    public static void checkTemporalWithZoneOrder(List<String> allDateTimes) {
        List<DateTimeValue> dateTimeValues = new ArrayList<>();
        for (String datetimeInput : allDateTimes) {
            dateTimeValues.add(DateTimeValue.parse(datetimeInput, ZoneId::systemDefault));
        }
        DateTimeValue previousDateTime = null;
        for (DateTimeValue datetime : dateTimeValues) {
            if (previousDateTime != null) {
                assertThat(Values.COMPARATOR.compare(previousDateTime, datetime))
                        .isNegative();
                assertThat(Values.COMPARATOR.compare(datetime, previousDateTime))
                        .isPositive();
                assertThat(Lucene10ValueFields.storedFromTemporal(previousDateTime))
                        .isLessThan(Lucene10ValueFields.storedFromTemporal(datetime));
            }
            previousDateTime = datetime;
        }
    }
}
