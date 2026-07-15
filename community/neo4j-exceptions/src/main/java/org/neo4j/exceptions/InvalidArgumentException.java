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

import static java.lang.String.format;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.messages.MessageUtil;

public class InvalidArgumentException extends Neo4jException {
    @Deprecated
    public InvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidArgumentException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    @Deprecated
    public InvalidArgumentException(String message) {
        super(message);
    }

    public InvalidArgumentException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    @Override
    public Status status() {
        return Status.Statement.ArgumentError;
    }

    public static InvalidArgumentException createRelationshipMissingNode(String relName, String nodeName) {
        var gql = GqlHelper.getGql22G03_22N01("NULL", List.of("NODE"), "NULL");
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Failed to create relationship `%s`, node `%s` is missing. If you prefer to simply ignore rows "
                                + "where a relationship node is missing, set 'dbms.cypher.lenient_create_relationship = true' in neo4j.conf",
                        relName, nodeName));
    }

    public static InvalidArgumentException unknownNormalForm(String normalForm) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N49)
                        .withParam(GqlParams.StringParam.input, normalForm)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "Unknown normal form. Valid values are: NFC, NFD, NFKC, NFKD.");
    }

    public static InvalidArgumentException incompleteSpatialValue(
            String crs, String mandatoryKeys, List<String> mandatoryKeysList) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N18)
                        .withParam(GqlParams.StringParam.crs, String.valueOf(crs))
                        .withParam(GqlParams.ListParam.mapKeyList, mandatoryKeysList)
                        .build())
                .build();
        return new InvalidArgumentException(gql, String.format("A %s point must contain %s", crs, mandatoryKeys));
    }

    public static InvalidArgumentException invalidSpatialValueCombination() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N22)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "Cannot specify both CRS and SRID");
    }

    public static InvalidArgumentException timezoneAndOffsetMismatch(
            String context, String offset, List<String> validOffsets, String matcherGroup) {
        ErrorGqlStatusObject gql;
        if (validOffsets.isEmpty()) {
            // This is an indication that the provided time and timezone combination is invalid
            gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                    .withParam(GqlParams.StringParam.value, String.valueOf(offset))
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                            .withParam(GqlParams.StringParam.input, String.valueOf(offset))
                            .withParam(GqlParams.StringParam.context, context)
                            .build())
                    .build();
        } else {
            gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                    .withParam(GqlParams.StringParam.value, String.valueOf(offset))
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                            .withParam(GqlParams.StringParam.input, String.valueOf(offset))
                            .withParam(GqlParams.StringParam.context, context)
                            .withParam(GqlParams.ListParam.inputList, validOffsets)
                            .build())
                    .build();
        }
        return new InvalidArgumentException(gql, "Timezone and offset do not match: " + matcherGroup);
    }

    public static InvalidArgumentException temporalSelectionConflict(String temporal1, String temporal2) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N14)
                        .withParam(GqlParams.StringParam.temporal1, temporal1)
                        .withParam(GqlParams.StringParam.temporal2, temporal2)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, String.format("%s cannot be selected together with %s.", temporal1, temporal2));
    }

    public static InvalidArgumentException invalidCoordinateNames() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N19)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'");
    }

    public static InvalidArgumentException pointWithWrongDimensions(int expectedDimension, int actualDimension) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N20)
                        .withParam(GqlParams.NumberParam.dim1, expectedDimension)
                        .withParam(GqlParams.NumberParam.value, actualDimension)
                        .withParam(GqlParams.NumberParam.dim2, actualDimension)
                        .build())
                .build();

        return new InvalidArgumentException(
                gql,
                String.format(
                        "Cannot create point with %dD coordinate reference system and %d coordinates. "
                                + "Please consider using equivalent %dD coordinate reference system",
                        expectedDimension, actualDimension, actualDimension));
    }

    public static InvalidArgumentException bothAllowedAndDeniedDbs() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N85)
                .build();
        return new InvalidArgumentException(gql, "Can't specify both allowed and denied databases");
    }

    public static InvalidArgumentException invalidEncryptionVersion(String version, List<String> allowedVersions) {
        var gql = GqlHelper.getGql42001_22N04(version, "encryption version", allowedVersions);
        var innerException = new IllegalArgumentException("The encryption version specified is not available.");
        return new InvalidArgumentException(gql, innerException.getMessage(), innerException);
    }

    public static InvalidArgumentException invalidURLScheme(String url, List<String> allowedSchemes) {
        var gql = GqlHelper.getGql42001_22N04(url, "URL scheme", allowedSchemes);
        return new InvalidArgumentException(gql, MessageUtil.invalidScheme(url, allowedSchemes));
    }

    public static InvalidArgumentException insecureURLScheme(String url, List<String> allowedSchemes) {
        var gql = GqlHelper.getGql42001_22N04(url, "URL scheme", allowedSchemes);
        return new InvalidArgumentException(gql, MessageUtil.insecureScheme(url, allowedSchemes));
    }

    public static InvalidArgumentException incorrectPasswordFormat() {
        var innerException = new IllegalArgumentException(
                "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'.");
        var gql = GqlHelper.getGql42001_22N04("*", "password format", List.of("'<encryption-version>,<hash>,<salt>'"));
        return new InvalidArgumentException(gql, innerException.getMessage(), innerException);
    }

    public static InvalidArgumentException duplicateFieldNotAllowed(String key) {
        var gql = GqlHelper.getGql22007_22N12(key);
        return new InvalidArgumentException(gql, String.format("Duplicate field '%s' is not allowed.", key));
    }

    public static InvalidArgumentException duplicateField(String key) {
        var gql = GqlHelper.getGql22007_22N12(key);
        return new InvalidArgumentException(gql, format("Duplicate field '%s'", key));
    }

    public static InvalidArgumentException parseMapValue(String text) {
        var gql = GqlHelper.getGql22007_22N12(text);
        return new InvalidArgumentException(gql, format("Failed to parse map value: '%s'", text));
    }

    public static InvalidArgumentException setTimeZoneTwice(String value) {
        var gql = GqlHelper.getGql22007_22N12(value);
        return new InvalidArgumentException(gql, "Cannot set timezone twice");
    }

    public static InvalidArgumentException unsupportedHeader(String key, String value) {
        var gql = GqlHelper.getGql22007_22N12(key);
        return new InvalidArgumentException(gql, "Unsupported header field: " + value);
    }

    public static InvalidArgumentException alterRemoteAliasToLocal(String alias) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N91)
                .withParam(GqlParams.StringParam.alias, alias)
                .build();
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Failed to alter the specified database alias '%s': alter a local alias to a remote alias is not supported.",
                        alias));
    }

    public static InvalidArgumentException alterLocalAliasToRemote(String alias) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N91)
                .withParam(GqlParams.StringParam.alias, alias)
                .build();
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Failed to alter the specified database alias '%s': alter a remote alias to a local alias is not supported.",
                        alias));
    }

    public static InvalidArgumentException renameEntityNotFound(
            ErrorGqlStatusObject gql, String entity, String fromName, String toName) {
        return new InvalidArgumentException(
                gql,
                String.format("Failed to rename the specified %s '%s' to ", entity, fromName)
                        + String.format("'%s': The %s '%s' does not exist.", toName, entity, fromName));
    }

    public static InvalidArgumentException renameEntityAlreadyExists(
            ErrorGqlStatusObject gql, String entity, String fromName, String toName, Throwable cause) {
        return new InvalidArgumentException(
                gql,
                String.format("Failed to rename the specified %s '%s' to ", entity.toLowerCase(Locale.ROOT), fromName)
                        + String.format("'%s': %s '%s' already exists.", toName, entity, toName),
                cause);
    }

    public static InvalidArgumentException oldPasswordEqualsNew(String user, Boolean onSelf) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, "***")
                .withParam(GqlParams.StringParam.context, user + " password")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N89)
                        .build())
                .build();
        var msg = onSelf
                ? String.format(
                        "User '%s' failed to alter their own password: Old password and new password cannot be the same.",
                        user)
                : String.format(
                        "Failed to alter the specified user '%s': Old password and new password cannot be the same.",
                        user);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException shortPassword(int minLength) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, "***")
                .withParam(GqlParams.StringParam.context, "password")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N85)
                        .withParam(GqlParams.NumberParam.lower, minLength)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, String.format("A password must be at least %s characters.", minLength));
    }

    public static InvalidArgumentException parameterizedDbWildcards(String syntax, String messageStart) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N86)
                .withParam(GqlParams.StringParam.syntax, syntax)
                .build();
        return new InvalidArgumentException(
                gql,
                String.format("%s: Parameterized database and graph names do not support wildcards.", messageStart));
    }

    public static InvalidArgumentException missingMandatoryAuthClause(
            String clause, String authProvider, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N97)
                .withParam(GqlParams.StringParam.clause, clause)
                .withParam(GqlParams.StringParam.auth, authProvider)
                .build();
        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException couldNotGetPassword() {
        var msg = "Could not get password name field from password expression.";
        var gql = GqlHelper.get50N00(InvalidArgumentException.class.getSimpleName(), msg);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException remoteDatabaseUrl() {
        var msg = "Could not validate remote database alias url.";
        var gql = GqlHelper.get50N00(InvalidArgumentException.class.getSimpleName(), msg);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException alterMissingUser(String username) {
        var gql = GqlHelper.getGql42002_42N09(username);
        return new InvalidArgumentException(
                gql, String.format("Failed to alter the specified user '%s': User does not exist.", username));
    }

    public static InvalidArgumentException roleMissingUser(String role, String username, Throwable cause) {
        var gql = GqlHelper.getGql42002_42N09(username);
        return new InvalidArgumentException(
                gql,
                String.format("Failed to grant role '%s' to user '%s': User does not exist.", role, username),
                cause);
    }

    public static InvalidArgumentException compositeAlias(String operationType, String alias, String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA6)
                .build();
        var msg = String.format("Failed to %s the specified database alias '%s': ", operationType, alias)
                + String.format("Database '%s' is composite.", dbName);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException dbNameTooLong(String dbName) {
        var gql = GqlHelper.getGql22N05_22N84(dbName, "database name", 65534);
        return new InvalidArgumentException(
                gql, "The provided target database name is to long, maximum characters are 65534.");
    }

    public static InvalidArgumentException aliasTooLong(String alias) {
        var gql = GqlHelper.getGql22N05_22N84(alias, "alias", 65534);
        return new InvalidArgumentException(gql, "The provided alias is to long, maximum characters are 65534.");
    }

    public static InvalidArgumentException tooManySeeders(int numberOfSeedingServers, int numberOfAllocations) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N18)
                        .withParam(GqlParams.NumberParam.countSeeders, numberOfSeedingServers)
                        .withParam(GqlParams.NumberParam.countAllocs, numberOfAllocations)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql,
                format(
                        "The number of seeding servers '%s' is larger than the defined number of allocations '%s'.",
                        numberOfSeedingServers, numberOfAllocations));
    }

    public static InvalidArgumentException noSuchSeeder(String serverId) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N19)
                        .withParam(GqlParams.StringParam.server, serverId)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, format("The specified seeding server with id '%s' could not be found.", serverId));
    }

    public static InvalidArgumentException topologyOutOfRange(
            String serverType, int constrainedServers, String allocationType, int desiredAllocations) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N56)
                        .withParam(GqlParams.StringParam.serverType, serverType)
                        .withParam(GqlParams.NumberParam.count1, constrainedServers)
                        .withParam(GqlParams.StringParam.allocType, allocationType)
                        .withParam(GqlParams.NumberParam.count2, desiredAllocations)
                        .build())
                .build();

        String formattedServerType;
        if (serverType.isEmpty()) {
            formattedServerType = "";
        } else {
            formattedServerType = " " + serverType;
        }

        String formattedAllocationType;
        if (allocationType.isEmpty()) {
            formattedAllocationType = "";
        } else {
            formattedAllocationType = " " + allocationType;
        }

        return new InvalidArgumentException(
                gql,
                String.format(
                        "The number of%s seeding servers '%s', is larger than the desired number of%s allocations '%s'.",
                        formattedServerType, constrainedServers, formattedAllocationType, desiredAllocations));
    }

    public static InvalidArgumentException notAValidCidrIp(String wrongIp, String legacyMessage, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, wrongIp)
                .withParam(GqlParams.StringParam.context, "CIDR IP")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N88)
                        .withParam(GqlParams.StringParam.input, wrongIp)
                        .build())
                .build();
        return new InvalidArgumentException(gql, legacyMessage, cause);
    }

    public static InvalidArgumentException mustSpecifyField(String mustAssign) {
        var gql = GqlHelper.getGql22007_22N12(mustAssign);
        return new InvalidArgumentException(gql, mustAssign + " must be specified");
    }

    public static InvalidArgumentException cannotReassign(String newValue, String field) {
        var gql = GqlHelper.getGql22007_22N12(newValue);
        return new InvalidArgumentException(gql, "cannot re-assign " + field);
    }

    public static InvalidArgumentException cannotAssignNonStringTimezone(
            String value, String field, String prettyValue) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, prettyValue)
                        .withParam(GqlParams.StringParam.variable, "timezone")
                        .withParam(GqlParams.ListParam.valueTypeList, List.of("STRING"))
                        .build())
                .build();
        return new InvalidArgumentException(gql, String.format("Cannot assign %s to field %s", value, field));
    }

    public static InvalidArgumentException cannotAssignPointField(
            String value, String field, String prettyValue, List<String> expectedTypes) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, prettyValue)
                        .withParam(GqlParams.StringParam.variable, "coordinate " + field.toLowerCase(Locale.ROOT))
                        .withParam(GqlParams.ListParam.valueTypeList, expectedTypes)
                        .build())
                .build();
        return new InvalidArgumentException(gql, String.format("Cannot assign %s to field %s", value, field));
    }

    public static InvalidArgumentException assignTimezoneTwice(String timezone) {
        var gql = GqlHelper.getGql22007_22N12(timezone);
        return new InvalidArgumentException(gql, "Cannot assign timezone twice.");
    }

    public static InvalidArgumentException noSuchTemporalField(String field) {
        var gql = GqlHelper.getGql22007_22N12(field);
        return new InvalidArgumentException(gql, "No such field: " + field);
    }

    public static InvalidArgumentException noSuchPointField(String field) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N11)
                .withParam(GqlParams.StringParam.input, field)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                        .withParam(GqlParams.StringParam.input, field)
                        .withParam(GqlParams.StringParam.context, "coordinate")
                        .withParam(
                                GqlParams.ListParam.inputList,
                                List.of("x", "y", "z", "longitude", "latitude", "height", "crs", "srid"))
                        .build())
                .build();
        return new InvalidArgumentException(gql, "No such field: " + field);
    }

    public static InvalidArgumentException cannotSpecifyWithout(String value, String missing) {
        var gql = GqlHelper.getGql22007_22N12(value);
        return new InvalidArgumentException(gql, value + " cannot be specified without " + missing);
    }

    public static InvalidArgumentException invalidMillisecondValue(long upper, long value) {
        var gql = GqlHelper.getGql22007_22N03("Millisecond", "INTEGER", 0, upper, value);
        return new InvalidArgumentException(gql, "Invalid value for Millisecond: " + value);
    }

    public static InvalidArgumentException invalidMicrosecondValue(long upper, long value) {
        var gql = GqlHelper.getGql22007_22N03("Microsecond", "INTEGER", 0, upper, value);
        return new InvalidArgumentException(gql, "Invalid value for Microsecond: " + value);
    }

    public static InvalidArgumentException invalidNanosecondValue(long upper, long value) {
        var gql = GqlHelper.getGql22007_22N03("Nanosecond", "INTEGER", 0, upper, value);
        return new InvalidArgumentException(gql, "Invalid value for Nanosecond: " + value);
    }

    public static InvalidArgumentException invalidNanosecond(long upper, long value) {
        var gql = GqlHelper.getGql22007_22N03("Nanosecond", "INTEGER", 0, upper, value);
        return new InvalidArgumentException(gql, "Invalid nanosecond: " + value);
    }

    public static InvalidArgumentException zeroStepRange() {
        var gql = GqlHelper.getGql22N38_22N03("range", "step", "INTEGER", 1, Long.MAX_VALUE, String.valueOf(0));
        return new InvalidArgumentException(gql, "Step argument to 'range()' cannot be zero");
    }

    public static InvalidArgumentException negRoundPrecision(int precision) {
        var gql = GqlHelper.getGql22N38_22N03("round", "precision", "INTEGER", 0, Long.MAX_VALUE, precision);
        return new InvalidArgumentException(gql, "Precision argument to 'round()' cannot be negative");
    }

    public static InvalidArgumentException tooManyWeeksThisYear(int week, int year) {
        var gql = GqlHelper.getGql22007_22N03("week of year " + year, "INTEGER", 1, 52, week);
        return new InvalidArgumentException(gql, String.format("Year %d does not contain %d weeks.", year, week));
    }

    public static InvalidArgumentException temporalOutOfRange(ChronoUnit unit, long value) {
        String msg;
        ErrorGqlStatusObject gql;
        switch (unit) {
            case MONTHS -> {
                msg = "months is out of range: " + value;
                gql = GqlHelper.getGql22007_22N03("month", "INTEGER", 1, 12, value);
            }
            case DAYS -> {
                msg = "days is out of range: " + value;
                gql = GqlHelper.getGql22007_22N03("day", "INTEGER", 1, 31, value);
            }
            case HOURS -> {
                msg = "hours out of range: " + value;
                gql = GqlHelper.getGql22007_22N03("hours", "INTEGER", 0, 24, value);
            }
            case MINUTES -> {
                msg = "minutes out of range: " + value;
                gql = GqlHelper.getGql22007_22N03("minutes", "INTEGER", 0, 60, value);
            }
            case SECONDS -> {
                msg = "seconds out of range: " + value;
                gql = GqlHelper.getGql22007_22N03("seconds", "INTEGER", 0, 60, value);
            }
            default -> {
                msg = unit.name().toLowerCase() + "out of range: " + value;
                gql = GqlHelper.getGql22007_22N03(unit.name().toLowerCase(), "INTEGER", -1, -1, value);
            }
        }
        ;
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException only91DaysInQuarter2(int year, int quarterDay) {
        var gql = GqlHelper.getGql22007_22N03("Day of Quarter 2 of year " + year, "INTEGER", 1, 91, quarterDay);
        return new InvalidArgumentException(gql, "Quarter 2 only has 91 days.");
    }

    public static InvalidArgumentException only90Or91DaysInQuarter1(int year, int quarterDay, int dayLimit) {
        var gql = GqlHelper.getGql22007_22N03("Day of Quarter 1 of year " + year, "INTEGER", 1, dayLimit, quarterDay);
        return new InvalidArgumentException(gql, String.format("Quarter 1 of %d only has %d days.", year, dayLimit));
    }

    public static InvalidArgumentException invalidValuePrefixSuffix(
            long minValue, String actualValue, String prefix, String suffix, String actualValuePretty) {
        var gql = GqlHelper.getGql22003_22N03("value", "INTEGER", minValue, Long.MAX_VALUE, actualValuePretty);
        return new InvalidArgumentException(
                gql, String.format("%s: Invalid input. '%s' is not a valid value.%s", prefix, actualValue, suffix));
    }

    public static InvalidArgumentException invalidDoubleValuePrefixSuffix(
            String actualValue, String prefix, String suffix, String actualValuePretty) {
        var gql = GqlHelper.getGql22G03_22N01(actualValuePretty, List.of("INTEGER"), "FLOAT");
        return new InvalidArgumentException(
                gql, String.format("%s: Invalid input. '%s' is not a valid value.%s", prefix, actualValue, suffix));
    }

    public static InvalidArgumentException countNotPosInt(Number actualCount) {
        var gql = GqlHelper.getGql22003_22N03("value", "INTEGER", 0, Long.MAX_VALUE, String.valueOf(actualCount));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Invalid input. '%s' is not a valid value. Must be a non-negative integer.", actualCount));
    }

    public static InvalidArgumentException countDoubleNotPosInt(double actualCount) {
        var gql = GqlHelper.getGql22G03_22N01(String.valueOf(actualCount), List.of("INTEGER"), "FLOAT");
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Invalid input. '%s' is not a valid value. Must be a non-negative integer.", actualCount));
    }

    public static InvalidArgumentException invalidPercentage(double faultyPercentage) {
        var gql = GqlHelper.getGql22003_22N03("percentage", "FLOAT", 0, 1, String.valueOf(faultyPercentage));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Invalid input '%s' is not a valid argument, must be a number in the range 0.0 to 1.0",
                        faultyPercentage));
    }

    public static InvalidArgumentException cannotConstructTemporal(String temporal, String got, String gotPretty) {
        String cypherType =
                switch (temporal) {
                    case "date" -> "DATE";
                    case "time" -> "ZONED TIME";
                    case "date time" -> "ZONED DATETIME";
                    case "local date time" -> "LOCAL DATETIME";
                    case "local time" -> "LOCAL TIME";
                    default -> temporal;
                };
        var gql = GqlHelper.getGql22007_22N25(cypherType, gotPretty);
        return new InvalidArgumentException(gql, String.format("Cannot construct %s from: %s", temporal, got));
    }

    public static InvalidArgumentException needIntegerOrFloat(String gotPretty, String gotType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotType);
        return new InvalidArgumentException(gql, "Factor must be either integer of floating point number.");
    }

    public static InvalidArgumentException inputContainsInvalidCharacters(
            String invalidInput, String context, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, invalidInput)
                .withParam(GqlParams.StringParam.context, context)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
                        .withParam(GqlParams.StringParam.input, invalidInput)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException failedConvertFunction(String function, Throwable cause) {
        var gql = GqlHelper.getGql22000_22N11(function);
        return new InvalidArgumentException(gql, cause.getMessage(), cause);
    }

    public static InvalidArgumentException listTooLarge(long size, long maxSize) {
        var gql = GqlHelper.getGql22003_22N03("list size", "INTEGER", 0, maxSize, String.valueOf(size));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Cannot populate list values larger than %d elements. Requested size: %d", maxSize, size));
    }
}
