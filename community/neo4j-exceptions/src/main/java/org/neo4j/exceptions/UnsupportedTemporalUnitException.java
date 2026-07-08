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
package org.neo4j.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

/**
 * {@code UnsupportedTemporalUnitException} is thrown if trying to get or assign a temporal unit
 * which is not supported for the current temporal type. Examples of such cases include trying to
 * assign a month to a {@code TimeValue}, trying to truncate a {@code DateValue} to minutes and
 * trying to get the timezone of a {@code LocalDateTimeValue}.
 */
public class UnsupportedTemporalUnitException extends CypherTypeException {
    private UnsupportedTemporalUnitException(ErrorGqlStatusObject gqlStatusObject, String errorMsg) {
        super(gqlStatusObject, errorMsg);
    }

    private UnsupportedTemporalUnitException(ErrorGqlStatusObject gqlStatusObject, String errorMsg, Throwable cause) {
        super(gqlStatusObject, errorMsg, cause);
    }

    public static UnsupportedTemporalUnitException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new UnsupportedTemporalUnitException(gql, message);
    }

    public static UnsupportedTemporalUnitException cannotProcess(String value, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N11)
                        .withParam(GqlParams.StringParam.input, value)
                        .build())
                .build();
        return new UnsupportedTemporalUnitException(gql, e.getMessage(), e);
    }

    public static UnsupportedTemporalUnitException cannotSelectDatetime(String dateTime) {
        var gql = GqlHelper.getGql22G05_22N15("ZONED DATETIME", dateTime);
        return new UnsupportedTemporalUnitException(gql, "Cannot select datetime from: " + dateTime);
    }

    public static UnsupportedTemporalUnitException tooSmallUnitForTruncate(String unit, String value) {
        var gql = GqlHelper.getGql22G05_22N15(unit, value);
        return new UnsupportedTemporalUnitException(gql, "Unit too small for truncation: " + unit);
    }

    public static UnsupportedTemporalUnitException cannotGetLocalTime(String value) {
        var gql = GqlHelper.getGql22G05_22N15("LOCAL TIME", value);
        return new UnsupportedTemporalUnitException(gql, String.format("Cannot get the time of: %s", value));
    }

    public static UnsupportedTemporalUnitException cannotGetZonedTime(String value) {
        var gql = GqlHelper.getGql22G05_22N15("ZONED TIME", value);
        return new UnsupportedTemporalUnitException(gql, String.format("Cannot get the time of: %s", value));
    }

    public static UnsupportedTemporalUnitException cannotGetTimezone(String value) {
        var gql = GqlHelper.getGql22G05_22N15("ZONED TIME/ZONED DATETIME", value);
        return new UnsupportedTemporalUnitException(gql, String.format("Cannot get the time zone of: %s", value));
    }

    public static UnsupportedTemporalUnitException cannotGetDate(String value) {
        var gql = GqlHelper.getGql22G05_22N15(
                "DATE",
                value); // should this also have something like "DATE/LOCAL DATETIME/ZONED DATETIME" or are they treated
        // separately?
        return new UnsupportedTemporalUnitException(gql, String.format("Cannot get the date of: %s", value));
    }

    public static UnsupportedTemporalUnitException cannotTruncateWithTimeBasedUnit(
            String input, String type, String prettifiedType) {
        var gql = GqlHelper.getGql22G05_22N15(prettifiedType, input);
        return new UnsupportedTemporalUnitException(
                gql, String.format("Cannot truncate %s to %s with a time based unit.", input, type));
    }

    public static UnsupportedTemporalUnitException cannotGetZoneOffset(String input) {
        var gql = GqlHelper.getGql22G05_22N15("ZONED TIME/ZONED DATETIME", input);
        return new UnsupportedTemporalUnitException(gql, String.format("Cannot get the offset of: %s", input));
    }

    public static UnsupportedTemporalUnitException noSuchField(String fieldName, String fieldType) {
        var gql = GqlHelper.getGql22G05_22N15(fieldName, fieldType);
        return new UnsupportedTemporalUnitException(gql, String.format("No such field: %s", fieldName));
    }

    public static UnsupportedTemporalUnitException epochNotSupported(String component, String temporal) {
        var gql = GqlHelper.getGql22G05_22N15(component, temporal);
        return new UnsupportedTemporalUnitException(gql, "Epoch not supported.");
    }

    public static UnsupportedTemporalUnitException notSupported(String component, String valueType) {
        var gql = GqlHelper.getGql22G05_22N40(component, valueType);
        return new UnsupportedTemporalUnitException(gql, "Not supported: " + component);
    }

    public static UnsupportedTemporalUnitException cannotAssignTimezone(String component, String valueType) {
        var gql = GqlHelper.getGql22G05_22N40(component, valueType);
        return new UnsupportedTemporalUnitException(gql, "Cannot assign time zone if also assigning other fields.");
    }

    public static UnsupportedTemporalUnitException cannotAssignToCalendarDate(String component) {
        var gql = GqlHelper.getGql22G05_22N40(component, "calendar date");
        return new UnsupportedTemporalUnitException(gql, "Cannot assign " + component + " to calendar date.");
    }

    public static UnsupportedTemporalUnitException cannotAssignToWeekDate(String component) {
        var gql = GqlHelper.getGql22G05_22N40(component, "week date");
        return new UnsupportedTemporalUnitException(gql, "Cannot assign " + component + " to week date.");
    }

    public static UnsupportedTemporalUnitException cannotAssignToQuarterDate(String component) {
        var gql = GqlHelper.getGql22G05_22N40(component, "quarter date");
        return new UnsupportedTemporalUnitException(gql, "Cannot assign " + component + " to quarter date.");
    }

    public static UnsupportedTemporalUnitException cannotAssignToOrdinalDate(String component) {
        var gql = GqlHelper.getGql22G05_22N40(component, "ordinal date");
        return new UnsupportedTemporalUnitException(gql, "Cannot assign " + component + " to ordinal date.");
    }

    public static UnsupportedTemporalUnitException cannotAssignTemporalField(
            String component, String valueType, Throwable cause) {
        var gql = GqlHelper.getGql22G06_22N40(component, valueType);
        return new UnsupportedTemporalUnitException(
                gql, String.format("Cannot assign %s of a %s.", component, valueType), cause);
    }

    public static UnsupportedTemporalUnitException cannotAssignDurationField(
            String component, String valueType, Throwable cause) {
        var gql = GqlHelper.getGql22G08_22N40(component, valueType);
        return new UnsupportedTemporalUnitException(
                gql, String.format("Cannot assign %s of a %s.", component, valueType), cause);
    }
}
