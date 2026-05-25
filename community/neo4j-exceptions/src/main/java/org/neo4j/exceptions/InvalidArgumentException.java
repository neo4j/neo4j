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
import static org.neo4j.gqlstatus.GqlHelper.getGql22G03_22N27;
import static org.neo4j.gqlstatus.GqlHelper.getGql42N51;
import static org.neo4j.gqlstatus.PrivilegeGqlCodeEntity.entityAlreadyExists;
import static org.neo4j.gqlstatus.PrivilegeGqlCodeEntity.entityNotFound;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.PrivilegeGqlCodeEntity;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.messages.MessageUtil;
import org.neo4j.util.CalledFromGeneratedCode;

public class InvalidArgumentException extends Neo4jException {

    public InvalidArgumentException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public InvalidArgumentException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    @Override
    public Status status() {
        return Status.Statement.ArgumentError;
    }

    public static InvalidArgumentException invalidFunctionArgument(String functionName, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N38)
                .withParam(GqlParams.StringParam.value, functionName)
                .build();
        return new InvalidArgumentException(gql, message);
    }

    public static InvalidArgumentException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new InvalidArgumentException(gql, message);
    }

    public static InvalidArgumentException internalError(String msgTitle, String message, Throwable cause) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new InvalidArgumentException(gql, message, cause);
    }

    public static InvalidArgumentException cannotImpersonateUser(String userToImpersonate) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidArgumentException(
                gql, String.format("%s '%s'.", "Cannot impersonate user", userToImpersonate));
    }

    public static InvalidArgumentException cannotImpersonateFromAnAlreadyImpersonatedContext() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidArgumentException(gql, "Cannot impersonate a user from an already impersonated context");
    }

    public static InvalidArgumentException unsupportedInCommunity(String feature) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                .withParam(GqlParams.StringParam.feat, feature)
                .withParam(GqlParams.StringParam.edition, "community edition")
                .build();
        return new InvalidArgumentException(gql, "%s is not supported in community edition.".formatted(feature));
    }

    public static InvalidArgumentException cdcUnexpectedFieldException(
            List<String> unexpected, List<String> expected, String context) {
        var expectedString = expected.stream().sorted().collect(Collectors.joining("', '", "'", "'"));
        var unexpectedString = unexpected.stream().sorted().collect(Collectors.joining("', '", "'", "'"));
        var legacyMessage =
                "Unexpected field(s) [%s], expected one of [%s].".formatted(unexpectedString, expectedString);

        var sortedExpected = expected.stream().sorted().toList();
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                .withParam(GqlParams.StringParam.input, unexpected.getFirst())
                .withParam(GqlParams.StringParam.context, context)
                .withParam(GqlParams.ListParam.inputList, sortedExpected)
                .build();
        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException cdcUnexpectedValueException(
            String key, String found, List<String> expected) {
        var expectedString = expected.stream().sorted().collect(Collectors.joining("', '", "'", "'"));
        var legacyMessage =
                "Unexpected value '%s' for field '%s', expected one of [%s].".formatted(found, key, expectedString);

        var sortedExpected = expected.stream().sorted().toList();
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                .withParam(GqlParams.StringParam.input, found)
                .withParam(GqlParams.StringParam.context, "field '%s'".formatted(key))
                .withParam(GqlParams.ListParam.inputList, sortedExpected)
                .build();
        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException cdcWrongTypeException(Object obj, String in, Class<?> expected) {
        String foundType =
                Optional.ofNullable(obj).map(o -> o.getClass().getSimpleName()).orElse("null");
        String value = Optional.ofNullable(obj).map(Object::toString).orElse("");
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                .withParam(GqlParams.StringParam.value, value)
                .withParam(GqlParams.ListParam.valueTypeList, List.of(expected.getSimpleName()))
                .withParam(GqlParams.StringParam.valueType, foundType)
                .build();
        String legacyMessage = "Wrong type '%s'('%s') in '%s', expected '%s'."
                .formatted(foundType, value, in, expected.getSimpleName());

        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException cdcMissingFieldException(String key, String context) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N55)
                .withParam(GqlParams.StringParam.mapKey, key)
                .withParam(GqlParams.StringParam.field, context)
                .build();
        return new InvalidArgumentException(gql, "Missing field '%s'.".formatted(key));
    }

    public static InvalidArgumentException cdcUnexpectedSelectorType(String found, String expected, String in) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N67)
                .withParam(GqlParams.StringParam.selectorType1, found)
                .withParam(GqlParams.StringParam.input, in)
                .withParam(GqlParams.StringParam.selectorType2, expected)
                .build();
        throw new InvalidArgumentException(
                gql,
                "Unexpected selector type '%s' at %s, expected selector to be a %s".formatted(found, in, expected));
    }

    public static InvalidArgumentException unknownNormalForm(String normalForm) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N49)
                        .withParam(GqlParams.StringParam.input, normalForm)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "Unknown normal form. Valid values are: NFC, NFD, NFKC, NFKD.");
    }

    public static InvalidArgumentException invalidPatternCharacter(String type) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I65)
                        .withParam(GqlParams.StringParam.valueType, type)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql,
                String.format(
                        "An invalid character is used in the pattern. Verify that all characters are supported by `%s`.",
                        type));
    }

    public static InvalidArgumentException patternParsingFailed() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I66)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, "Pattern parsing failed. Make sure that an even number of escapes are used in the pattern.");
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

    public static InvalidArgumentException defaultLanguageForConstituentAliases() {
        var gql = GqlHelper.getGql42001_42N14_WithoutPosition("DEFAULT LANGUAGE", "constituent aliases");
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException defaultLanguageForLocalAliases() {
        var gql = GqlHelper.getGql42001_42N14_WithoutPosition("DEFAULT LANGUAGE", "local aliases");
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException renameEntityNotFound(
            PrivilegeGqlCodeEntity entity, String fromName, String toName, String fromParamName) {
        var action = "rename the specified %s '%s' to '%s'"
                .formatted(entity.description.toLowerCase(Locale.ROOT), fromName, toName);
        return failedActionEntityNotFound2(action, entity, fromName, fromParamName);
    }

    public static InvalidArgumentException renameEntityAlreadyExists(
            PrivilegeGqlCodeEntity entity, String fromName, String toName) {
        var action = "rename the specified %s '%s' to '%s'"
                .formatted(entity.description.toLowerCase(Locale.ROOT), fromName, toName);
        return failedActionEntityAlreadyExists2(action, entity, toName);
    }

    public static InvalidArgumentException createEntityAlreadyExists(PrivilegeGqlCodeEntity entity, String name) {
        var action = "create the specified %s '%s'".formatted(entity.description.toLowerCase(Locale.ROOT), name);
        return failedActionEntityAlreadyExists(action, entity, name);
    }

    public static InvalidArgumentException failedActionEntityNotFound(
            String action, PrivilegeGqlCodeEntity entity, String name, String paramName) {
        return failedActionEntityNotFound(action, entity, name, paramName, null);
    }

    public static InvalidArgumentException failedActionEntityNotFound(
            String action, PrivilegeGqlCodeEntity entity, String name, String paramName, String command) {
        // e.g. Failed to <delete the specified role 'myRole'>: <Role> does not exist.
        return new InvalidArgumentException(
                entityNotFound(entity, name, paramName, command),
                "Failed to %s: %s does not exist.".formatted(action, entity.description));
    }

    public static InvalidArgumentException failedActionEntityNotFound2(
            String action, PrivilegeGqlCodeEntity entity, String name, String paramName) {
        // e.g. Failed to <rename the role 'oldName' to 'newName'>: The <role> '<oldName>' does not exist.
        return new InvalidArgumentException(
                entityNotFound(entity, name, paramName),
                "Failed to %s: The %s '%s' does not exist."
                        .formatted(action, entity.description.toLowerCase(Locale.ROOT), name));
    }

    public static InvalidArgumentException failedActionEntityAlreadyExists(
            String action, PrivilegeGqlCodeEntity entity, String name) {
        // e.g. Failed to <create the specified user 'neo4j'>: <User> already exists.
        return new InvalidArgumentException(
                entityAlreadyExists(entity, name),
                "Failed to %s: %s already exists.".formatted(action, entity.description));
    }

    public static InvalidArgumentException failedActionEntityAlreadyExists2(
            String action, PrivilegeGqlCodeEntity entity, String name) {
        // e.g. Failed to <rename the role 'oldName' to 'newName'>: <Role> '<newName>' already exists.
        return new InvalidArgumentException(
                entityAlreadyExists(entity, name),
                "Failed to %s: %s '%s' already exists.".formatted(action, entity.description, name));
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

    public static InvalidArgumentException invalidCredentialsDuringAlterPassword(String user) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidArgumentException(
                gql, "User '%s' failed to alter their own password: Invalid principal or credentials.".formatted(user));
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

    public static InvalidArgumentException providedStringEmpty(String capitalizedField) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                .withParam(GqlParams.StringParam.item, capitalizedField)
                .build();
        return new InvalidArgumentException(
                gql, String.format("The provided %s is empty.", capitalizedField.toLowerCase(Locale.ROOT)));
    }

    public static InvalidArgumentException providedStringEmpty(String capitalizedField, String legacyField) {
        // Only here to provide the legacy message for "The provided Alias is empty.".
        // Use providedStringEmpty(String capitalizedField) instead.
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                .withParam(GqlParams.StringParam.item, capitalizedField)
                .build();
        return new InvalidArgumentException(
                gql, String.format("The provided %s is empty.", legacyField.toLowerCase(Locale.ROOT)));
    }

    public static InvalidArgumentException providedPasswordEmpty() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                .withParam(GqlParams.StringParam.item, "Password")
                .build();
        return new InvalidArgumentException(gql, "A password cannot be empty.");
    }

    public static InvalidArgumentException notAllowedToBeEmptyString(String item) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                .withParam(GqlParams.StringParam.item, item)
                .build();
        return new InvalidArgumentException(
                gql, "Invalid input. %s is not allowed to be an empty string.".formatted(item));
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

    public static InvalidArgumentException alterMissingUser(String username, String paramName) {
        return failedActionEntityNotFound(
                "alter the specified user '%s'".formatted(username), PrivilegeGqlCodeEntity.USER, username, paramName);
    }

    public static InvalidArgumentException alterMissingAuthRule(String authRule, String paramName) {
        return failedActionEntityNotFound(
                "alter the specified auth rule '%s'".formatted(authRule),
                PrivilegeGqlCodeEntity.AUTHRULE,
                authRule,
                paramName);
    }

    public static InvalidArgumentException roleMissingUser(
            String role, String username, String paramName, String command) {
        return failedActionEntityNotFound(
                "grant role '%s' to user '%s'".formatted(role, username),
                PrivilegeGqlCodeEntity.USER,
                username,
                paramName,
                command);
    }

    public static InvalidArgumentException grantRoleToAuthRuleMissingAuthRule(
            String role, String username, String paramName, String command) {
        return failedActionEntityNotFound(
                "grant role '%s' to auth rule '%s'".formatted(role, username),
                PrivilegeGqlCodeEntity.AUTHRULE,
                username,
                paramName,
                command);
    }

    public static InvalidArgumentException invalidCommandMissingUser(
            String command, String username, String parameterName) {
        var gql = GqlHelper.get42N09_userNotFound(command, username, parameterName);
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException invalidCommandMissingAuthRule(
            String command, String username, String parameterName) {
        var gql = GqlHelper.get42NAD_authRuleNotFound(command, username, parameterName);
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException invalidCommandMissingRole(
            String command, String role, String parameterName) {
        var gql = GqlHelper.get42N10_roleNotFound(command, role, parameterName);
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException invalidCommandMissingRoleWithLegacyMessage(
            String msg, String command, String role, String parameterName) {
        var gql = GqlHelper.get42N10_roleNotFound(command, role, parameterName);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException invalidCommandDatabaseDoesNotExists(
            String command, String dbname, String parameterName) {
        var gql = GqlHelper.get42N00_databaseNotFound(command, dbname, parameterName);
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentException invalidCommandParameterizedDatabaseNameDoesNotSupportWildcards(
            String command) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N86)
                .withParam(GqlParams.StringParam.syntax, command)
                .build();
        return new InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql));
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
                .withParam(GqlParams.StringParam.value, String.valueOf(constrainedServers))
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

    public static InvalidArgumentException fieldNotAvailableOnPoint(
            String fieldName, String point, Boolean isCartesian) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N58)
                        .withParam(GqlParams.StringParam.component, fieldName)
                        .withParam(GqlParams.StringParam.value, point)
                        .build())
                .build();
        String cartesian = isCartesian ? "cartesian " : "";
        return new InvalidArgumentException(
                gql, String.format("Field: %s is not available on %spoint: %s", fieldName, cartesian, point));
    }

    public static InvalidArgumentException notAValidCidrIp(
            String wrongIp,
            Boolean cypher5,
            String legacyErrorMessage,
            String command,
            Throwable cause,
            String paramName) {
        var gqlBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, wrongIp)
                .withParam(GqlParams.StringParam.context, GqlParams.StringParam.cmd.process(command));

        var invalidParamCause = paramName == null
                ? null
                : ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N51)
                        .withParam(GqlParams.StringParam.param, paramName);

        var invalidCidrCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N88)
                .withParam(GqlParams.StringParam.input, wrongIp)
                .build();

        if (invalidParamCause != null) {
            gqlBuilder.withCause(invalidParamCause.withCause(invalidCidrCause).build());
        } else {
            gqlBuilder.withCause(invalidCidrCause);
        }

        var gql = gqlBuilder.build();
        String message = cypher5 ? legacyErrorMessage : gql.getMessage();

        return new InvalidArgumentException(gql, message, cause);
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
                        .withParam(GqlParams.StringParam.context, "timezone")
                        .withParam(GqlParams.ListParam.valueTypeList, List.of("STRING"))
                        .withParam(GqlParams.StringParam.hint, "")
                        .build())
                .build();
        return new InvalidArgumentException(gql, String.format("Cannot assign %s to field %s", value, field));
    }

    public static InvalidArgumentException invalidIndexConfig(String key, List<String> expected) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, key)
                        .withParam(GqlParams.StringParam.context, "index setting")
                        .withParam(GqlParams.ListParam.valueTypeList, expected)
                        .withParam(GqlParams.StringParam.hint, "")
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, String.format("Invalid index config key '%s', it was not recognized as an index setting.", key));
    }

    public static InvalidArgumentException noSuchFullTextAnalyzer(String analyzerName, List<String> expected) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, analyzerName)
                        .withParam(GqlParams.StringParam.context, "index setting")
                        .withParam(GqlParams.ListParam.valueTypeList, expected)
                        .withParam(GqlParams.StringParam.hint, "")
                        .build())
                .build();
        return new InvalidArgumentException(gql, "No such full-text analyzer: '" + analyzerName + "'.");
    }

    public static InvalidArgumentException cannotAssignPointField(
            String value, String field, String prettyValue, List<String> expectedTypes) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, prettyValue)
                        .withParam(GqlParams.StringParam.context, "coordinate " + field.toLowerCase(Locale.ROOT))
                        .withParam(GqlParams.ListParam.valueTypeList, expectedTypes)
                        .withParam(GqlParams.StringParam.hint, "")
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

    public static InvalidArgumentException invalidArgument(String msg, Throwable t) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .build();
        throw new InvalidArgumentException(gql, msg, t);
    }

    public static InvalidArgumentException argumentOutOfRange(
            String fun, String arg, long lower, long upper, long value) {
        var gql = GqlHelper.getGql22N38_22N03(fun, arg, "INTEGER", lower, upper, value);
        return new InvalidArgumentException(gql, String.format("Function argument to '%s()' is out of range", fun));
    }

    public static InvalidArgumentException zeroStepRange() {
        var gql = GqlHelper.getGql22N38_22N03("range", "step", "INTEGER", 1, Long.MAX_VALUE, 0);
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
                msg = unit.name().toLowerCase(Locale.ROOT) + "out of range: " + value;
                gql = GqlHelper.getGql22007_22N03(unit.name().toLowerCase(Locale.ROOT), "INTEGER", -1, -1, value);
            }
        }
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

    public static InvalidArgumentException durationBetweenNonTemporalValues(
            String value, List<String> valueTypeList, String valueType) {
        var gql = GqlHelper.getGql22007_22N01(value, valueTypeList, valueType);
        return new InvalidArgumentException(gql, "Can only compute durations between TemporalValues.");
    }

    public static InvalidArgumentException emptyBuilderState() {
        var gql = GqlHelper.getGql22007_22N12("null");
        return new InvalidArgumentException(gql, "Builder state empty");
    }

    public static InvalidArgumentException needIntegerOrFloat(String gotPretty, String gotType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotType);
        return new InvalidArgumentException(gql, "Factor must be either integer of floating point number.");
    }

    public static InvalidArgumentException invalidCRSForGeographic(String crs) {
        var gql = GqlHelper.getGql22000_22N21(crs);
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Geographic points does not support coordinate reference system: %s."
                                + "This is set either in the csv header or the actual data column",
                        crs));
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

    public static InvalidArgumentException incompleteAllocationPicking() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N57)
                .withParam(GqlParams.StringParam.msg, "incomplete")
                .build();
        return new InvalidArgumentException(gql, "Unexpected error while picking allocations - incomplete.");
    }

    public static InvalidArgumentException primaryExceededAllocationPicking() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N57)
                .withParam(GqlParams.StringParam.msg, "primary exceeded")
                .build();
        return new InvalidArgumentException(gql, "Unexpected error while picking allocations - primary exceeded.");
    }

    public static InvalidArgumentException secondaryExceededAllocationPicking() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N57)
                .withParam(GqlParams.StringParam.msg, "secondary exceeded")
                .build();
        return new InvalidArgumentException(gql, "Unexpected error while picking allocations - secondary exceeded.");
    }

    public static InvalidArgumentException resourceExhaustion(long desiredAllocations, long allocations) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N66)
                .build();
        return new InvalidArgumentException(
                gql,
                "Desired number of allocations is '" + desiredAllocations + "', but only '"
                        + (desiredAllocations - allocations)
                        + "' possible servers found - some servers may be constrained.");
    }

    public static InvalidArgumentException cannotChangeDefaultDb(String oldDatabaseName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, "dbms.setDefaultDatabase")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N12)
                        .withParam(GqlParams.StringParam.db, oldDatabaseName)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, String.format("The old default database '%s' is still running.", oldDatabaseName));
    }

    public static InvalidArgumentException newDefaultDbDoesNotExist(String databaseName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N13)
                        .withParam(GqlParams.StringParam.db, databaseName)
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, String.format("New default database '%s' does not exist.", databaseName));
    }

    public static InvalidArgumentException systemCannotBeDefaultDb() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N14)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "System database cannot be set as default.");
    }

    public static InvalidArgumentException nullArgumentNotAllowed() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22004)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "Null argument not allowed.");
    }

    public static InvalidArgumentException cannotDeallocateServers(
            Collection<String> servers, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N43)
                        .withParam(
                                GqlParams.ListParam.serverList, servers.stream().toList())
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();

        String serversString = servers.stream().collect(Collectors.joining(",", "'", "'"));
        var operation = "Could not deallocate server(s) " + serversString + ".";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotDropServer(String server, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N44)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not drop server '" + server + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotCordonServer(String server, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N45)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not cordon server '" + server + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotAlterServer(String server, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N46)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not alter server '" + server + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotRenameServer(String server, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N47)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not rename server '" + server + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotEnableServer(String server, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N48)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not enable server '" + server + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotAlterDatabase(
            String databaseName, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N49)
                        .withParam(GqlParams.StringParam.db, databaseName)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not alter database '" + databaseName + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotRecreateDatabase(
            String databaseName, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N50)
                        .withParam(GqlParams.StringParam.db, databaseName)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not recreate database '" + databaseName + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotCreateDatabase(
            String databaseName, boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N51)
                        .withParam(GqlParams.StringParam.db, databaseName)
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not create database '" + databaseName + "'.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    public static InvalidArgumentException cannotReallocate(boolean withCauseMessage, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N54)
                        .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                        .build())
                .withParam(GqlParams.StringParam.msg, topologyDetailMessage(withCauseMessage, e))
                .build();
        var operation = "Could not calculate reallocation for databases.";
        return createTopologyException(gql, operation, withCauseMessage, e);
    }

    @CalledFromGeneratedCode
    public static InvalidArgumentException entityShouldBeNodeOrRel(String entity, String resolvedEntity) {
        var gql = getGql22G03_22N27(resolvedEntity, entity, List.of("NODE", "RELATIONSHIP"));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "The expression %s should have been a node or a relationship, but got %s",
                        entity, resolvedEntity));
    }

    public static InvalidArgumentException impersonationNotSupportedWithAuthDisabled() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                .withParam(GqlParams.StringParam.item, "Impersonation")
                .withParam(GqlParams.StringParam.context, "a database with auth disabled")
                .build();
        return new InvalidArgumentException(gql, "Impersonation is not supported with auth disabled.");
    }

    public static InvalidArgumentException impersonationNotSupportedWithNativeAuthDisabled() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                .withParam(GqlParams.StringParam.item, "Impersonation")
                .withParam(GqlParams.StringParam.context, "a database with native auth disabled")
                .build();
        return new InvalidArgumentException(gql, "Cannot impersonate with native authorization disabled.");
    }

    public static InvalidArgumentException unsupportedWhenNotNative() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                .withParam(GqlParams.StringParam.item, "Changing username")
                .withParam(GqlParams.StringParam.context, "when using an authentication provider apart from native")
                .build();
        return new InvalidArgumentException(
                gql,
                "Changing username is not supported when using an authentication or authentication provider apart from native.");
    }

    public static InvalidArgumentException unsupportedWithoutSetting(
            String concept, String settingScope, String settingName, String settingValue) {
        var context = String.format("%s without configuration setting: %s=%s", settingScope, settingName, settingValue);
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                .withParam(GqlParams.StringParam.item, concept)
                .withParam(GqlParams.StringParam.context, context)
                .build();
        return new InvalidArgumentException(gql, concept + " is not supported in " + context);
    }

    public static InvalidArgumentException unsupportedOperation(String operation, String context) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N31)
                .withParam(GqlParams.StringParam.feat, operation)
                .withParam(GqlParams.StringParam.context, context)
                .build();
        return new InvalidArgumentException(gql, operation + " is not supported in " + context);
    }

    public static InvalidArgumentException pbacNotSupportedWithSPD() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N71)
                .withParam(
                        GqlParams.StringParam.feat,
                        "Property Based Access Control for MATCH and TRAVERSE privilege actions")
                .build();
        return new InvalidArgumentException(
                gql,
                "Property Based Access Control for MATCH and TRAVERSE privilege actions is not supported on a Sharded Database.");
    }

    public static InvalidArgumentException invalidGraphName(String graphName) {
        var msg = String.format(
                "Failed to parse `%s` as a graph name. Graph name parts that contain unsupported characters for unescaped identifiers require backtick escaping. Graph name parts with special characters may require additional escaping of those characters.\"",
                graphName);
        var gql = GqlHelper.get50N22(graphName);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException invalidOptionTypeForAlterUser(String parameter, String actualType) {
        var gql = getGql42N51(
                parameter,
                getGql22G03_22N27(
                        actualType, GqlParams.StringParam.cmd.process("ALTER USER"), List.of("BOOLEAN", "STRING")));

        return new InvalidArgumentException(
                gql,
                String.format(
                        "Invalid option type for ALTER USER, expected PasswordExpression, Boolean, String or Parameter but got: %s",
                        actualType));
    }

    public static InvalidArgumentException incorrectTypeForAllocationHint(String input, String hint, String type) {
        var legacyMessage = String.format(
                "Incorrect value type provided for allocation hint '%s'. Expected an Integer but found a %s.",
                hint, type);
        var gql = getGql22G03_22N27(input, hint, List.of("INTEGER"));
        return new InvalidArgumentException(gql, legacyMessage);
    }

    public static InvalidArgumentException invalidAllocationHintKey(String invalidKey, Set<String> validKeys) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA9)
                .withParam(GqlParams.StringParam.mapKey, invalidKey)
                .withParam(GqlParams.ListParam.mapKeyList, validKeys.stream().toList())
                .build();

        var validKeysString = validKeys.stream().collect(Collectors.joining(", ", "'", "'"));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "The key %s is not a recognised allocation hint key! Valid hint keys are: %s",
                        invalidKey, validKeysString));
    }

    public static InvalidArgumentException atLeastOneTemporalUnitRequired() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N30)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "At least one temporal unit must be specified.");
    }

    public static InvalidArgumentException cannotProcessTemporal(String input, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N11)
                        .withParam(GqlParams.StringParam.input, input)
                        .build())
                .build();
        return new InvalidArgumentException(gql, e.getMessage(), e);
    }

    public static InvalidArgumentException queryContainsIllegalName(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N51)
                        .withParam(GqlParams.StringParam.param, name)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N15)
                                .withParam(GqlParams.StringParam.syntax, name)
                                .build())
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, "The query contains a parameter with an illegal name: '%s'".formatted(name));
    }

    public static InvalidArgumentException invalidPrefixSystem(String entity, String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N15)
                        .withParam(GqlParams.StringParam.syntax, "system")
                        .build())
                .build();
        throw new InvalidArgumentException(
                gql, "%s '%s' is invalid, due to the prefix 'system'.".formatted(entity, name));
    }

    public static InvalidArgumentException failedActionReservedRole(String action, String role) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N15)
                        .withParam(GqlParams.StringParam.syntax, role)
                        .build())
                .build();
        return new InvalidArgumentException(gql, "Failed to %s: '%s' is a reserved role.".formatted(action, role));
    }

    public static InvalidArgumentException providerIdCombinationAlreadyInUseCreate(String username) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N95)
                .build();
        return new InvalidArgumentException(
                gql,
                "Failed to create the specified user '%s': The combination of provider and id is already in use."
                        .formatted(username));
    }

    public static InvalidArgumentException providerIdCombinationAlreadyInUseAlter(String username) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N95)
                .build();
        return new InvalidArgumentException(
                gql,
                "Failed to alter the specified user '%s': The combination of provider and id is already in use."
                        .formatted(username));
    }

    public static InvalidArgumentException atLeastOneAuthProviderRequired() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N96)
                .build();
        return new InvalidArgumentException(
                gql,
                "User has no auth provider. Add at least one auth provider for the user or consider suspending them.");
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

    public static InvalidArgumentException invalidValueInHistogramFromConfig(
            String fieldKey, String fieldValue, List<String> expected) {
        var legacyMessage = "Invalid input '%s' for %s. Expected %s.".formatted(fieldValue, fieldKey, expected);

        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                .withParam(GqlParams.StringParam.input, fieldValue)
                .withParam(GqlParams.StringParam.context, fieldKey)
                .withParam(GqlParams.ListParam.inputList, expected)
                .build();
        return new InvalidArgumentException(gql, legacyMessage);
    }

    private static String topologyDetailMessage(boolean withCauseMessage, Throwable e) {
        return withCauseMessage && e.getMessage() != null && !e.getMessage().isBlank()
                ? e.getMessage()
                : "Internal error";
    }

    private static InvalidArgumentException createTopologyException(
            ErrorGqlStatusObject gql, String operation, boolean withCauseMessage, Throwable e) {
        if (withCauseMessage && e.getMessage() != null && !e.getMessage().isBlank()) {
            return new InvalidArgumentException(gql, operation + " " + e.getMessage());
        }
        return new InvalidArgumentException(gql, operation, e);
    }

    public static InvalidArgumentException invalidVectorDimensions(
            int minDimensions, int maxDimensions, int givenDimensions) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBE)
                .withParam(GqlParams.NumberParam.count1, minDimensions)
                .withParam(GqlParams.NumberParam.count2, maxDimensions)
                .withParam(GqlParams.NumberParam.count3, givenDimensions)
                .build();
        return new InvalidArgumentException(gql, gql.getMessage());
    }

    public static InvalidArgumentException propertyValueTooBig(
            String typeDescription, long maxBytes, String propertyValue) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBF)
                .withParam(GqlParams.StringParam.typeDescription, typeDescription)
                .withParam(GqlParams.NumberParam.bytes, maxBytes)
                .withParam(GqlParams.StringParam.value, propertyValue)
                .build();
        return new InvalidArgumentException(gql, gql.getMessage());
    }

    public static InvalidArgumentException invalidVectorCoordinate(float[] coordinates) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBG)
                .withParam(GqlParams.StringParam.value, Arrays.toString(coordinates))
                .build();
        return new InvalidArgumentException(gql, gql.getMessage());
    }

    public static InvalidArgumentException invalidVectorCoordinate(double[] coordinates) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBG)
                .withParam(GqlParams.StringParam.value, Arrays.toString(coordinates))
                .build();
        return new InvalidArgumentException(gql, gql.getMessage());
    }

    public static InvalidArgumentException invalidVectorCoordinate(String msg, String got) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBG)
                .withParam(GqlParams.StringParam.value, got)
                .build();
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException listTooLarge(long size, long maxSize) {
        var gql = GqlHelper.getGql22003_22N03("list size", "INTEGER", 0, maxSize, String.valueOf(size));
        return new InvalidArgumentException(
                gql,
                String.format(
                        "Cannot populate list values larger than %d elements. Requested size: %d", maxSize, size));
    }

    public static InvalidArgumentException invalidType(
            String context, String value, String expectedType, String actualType) {
        var gql = GqlHelper.getGql22G03_22N01(value, List.of(expectedType), actualType);
        return new InvalidArgumentException(
                gql, String.format("Wrong type for %s. Expected %s, got %s", context, expectedType, actualType));
    }

    public static InvalidArgumentException invalidType(String value, String actualType, List<String> expectedTypes) {
        var gql = GqlHelper.getGql22G03_22N01(value, expectedTypes, actualType);
        return new InvalidArgumentException(gql, gql.toString());
    }

    public static InvalidArgumentException missingInput(String context, String... expected) {
        return missingInput(context, Arrays.asList(expected));
    }

    public static InvalidArgumentException missingInput(String context, List<String> expected) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
                .withParam(GqlParams.ListParam.inputList, expected)
                .build();
        var joiner = new StringJoiner(", ", "[", "]");
        expected.forEach(joiner::add);
        return new InvalidArgumentException(
                gql, "%s is expected to have been set. Expected %s".formatted(context, joiner.toString()));
    }

    public static InvalidArgumentException expectedString(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("STRING"), gotCypherType);
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException outOfRange(
            String component, String value, String valueType, String min, String max) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.lower, min)
                        .withParam(GqlParams.StringParam.upper, max)
                        .withParam(GqlParams.StringParam.value, String.valueOf(value))
                        .build())
                .build();
        return new InvalidArgumentException(
                gql, "'%s' must be between %s and %s inclusively".formatted(component, min, max));
    }

    public static InvalidArgumentException invalidIndexInput(String input, String context, String msg) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.StringParam.context, context)
                .build();
        return new InvalidArgumentException(gql, msg);
    }

    public static InvalidArgumentException wrongIndexType(
            String indexName, String expectedIndexType, String actualIndexType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NCG)
                .withParam(GqlParams.StringParam.idx, indexName)
                .withParam(GqlParams.StringParam.idxType1, expectedIndexType)
                .withParam(GqlParams.StringParam.idxType2, actualIndexType)
                .build();
        return new InvalidArgumentException(gql, gql.getMessage());
    }

    public static InvalidArgumentException integerNonNullOutOfBounds(
            String legacyMst, String component, Number lower, Number upper, String input) {
        var gql = GqlHelper.getGql22003_22N03(component, "INTEGER NOT NULL", lower, upper, input);
        return new InvalidArgumentException(gql, legacyMst);
    }
}
