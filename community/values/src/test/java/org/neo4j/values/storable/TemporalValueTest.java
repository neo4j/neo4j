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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.DateTimeValue.builder;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

class TemporalValueTest {

    private FrozenClock clock = new FrozenClock("UTC");

    @Test
    void shouldTruncateNicely() {
        TemporalValue t = datetime(0, 1, 1, 14, 0, 3, 0, "UTC");
        TemporalUnit TU = ChronoUnit.WEEKS;
        MapValue fields = EMPTY_MAP;
        var zone = ZoneId.of("UTC");

        assertThatThrownBy(() -> LocalTimeValue.truncate(TU, t, fields, () -> zone))
                .asInstanceOf(InstanceOfAssertFactories.type(ErrorGqlStatusObject.class))
                .satisfies(e -> assertEquals(
                        e.cause().get().statusDescription(),
                        "error: data exception - invalid argument. Invalid argument: cannot process 'Weeks'."));
    }

    @Test
    void shouldNotAcceptNonTemporalValuesInUntil() {
        TemporalValue<ZonedDateTime, DateTimeValue> t = datetime(0, 1, 1, 14, 0, 3, 0, "UTC");
        TemporalUnit unit = ChronoUnit.WEEKS;
        Temporal nonTemporalValue = Year.of(2025);

        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> t.until(nonTemporalValue, unit))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Can only compute durations between TemporalValues.")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22007)
                .hasStatusDescription("error: data exception - invalid date, time, or datetime format")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N01)
                .hasStatusDescription(
                        "error: data exception - invalid type. Expected the value 2025 to be of type DATE, LOCAL DATETIME, LOCAL TIME, ZONED DATETIME or ZONED TIME, but was of type java.time.temporal.Temporal.");
    }

    @Test
    void shouldGiveProperErrorForWrongTypedTemporalField() {
        MapValue map = VirtualValues.map(new String[] {"year"}, new AnyValue[] {booleanValue(true)});
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> DateValue.build(map, clock))
                .isInstanceOf(UnsupportedTemporalUnitException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G06)
                .hasStatusDescription("error: data exception - invalid date, time, or datetime function field value")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N40)
                .hasStatusDescription(
                        "error: data exception - non-assignable temporal component. Cannot assign 'year' of a BOOLEAN.");
    }

    @Test
    void shouldGiveProperErrorForWrongTypedTimezone() {
        MapValue map =
                VirtualValues.map(new String[] {"hour", "timezone"}, new AnyValue[] {longValue(12), longValue(123)});
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> TimeValue.build(map, clock))
                .isInstanceOf(UnsupportedTemporalUnitException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G06)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N40)
                .hasStatusDescription(
                        "error: data exception - non-assignable temporal component. Cannot assign 'timezone' of a INTEGER.");
    }

    @Test
    void shouldNotAcceptEmptyBuilderState() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> builder(clock).build())
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Builder state empty")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22007)
                .hasStatusDescription("error: data exception - invalid date, time, or datetime format")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N12)
                .hasStatusDescription(
                        "error: data exception - invalid date, time, or datetime format. Invalid argument: cannot process 'null'.");
    }
}
