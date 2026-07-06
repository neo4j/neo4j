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
package org.neo4j.kernel.api.exceptions;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.gqlstatus.GqlHelper.getGql22G03_22N27;
import static org.neo4j.gqlstatus.GqlHelper.getGql42N51;
import static org.neo4j.gqlstatus.GqlHelper.maybeInvalidParameter;
import static org.neo4j.gqlstatus.PrivilegeGqlCodeEntity.databasesAlreadyExists;
import static org.neo4j.values.utils.PrettyPrinter.stringify;

import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.schema.IndexSettingRecord;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

public class InvalidArgumentsException extends GqlException implements Status.HasStatus {
    private final Status status;

    public InvalidArgumentsException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, null);
    }

    private InvalidArgumentsException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
        this.status = Status.General.InvalidArguments;
    }

    @Override
    public Status status() {
        return status;
    }

    public static InvalidArgumentsException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new InvalidArgumentsException(gql, message);
    }

    public static InvalidArgumentsException databaseAlreadyExistsInSystemDb(String defaultName) {
        return new InvalidArgumentsException(
                databasesAlreadyExists(List.of(defaultName, SYSTEM_DATABASE_NAME)),
                "The specified database '" + defaultName + "' or '" + SYSTEM_DATABASE_NAME + "' already exists.");
    }

    public static InvalidArgumentsException invalidProcedureArgument(
            String providedInvalidArgument,
            String argumentName,
            String procedureName,
            String expectedFormat,
            String legacyMessage,
            Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N22)
                        .withParam(GqlParams.StringParam.field, providedInvalidArgument)
                        .withParam(GqlParams.StringParam.procParam, argumentName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .withParam(GqlParams.StringParam.procParam, argumentName)
                        .withParam(GqlParams.StringParam.procParamFmt, expectedFormat)
                        .build())
                .build();
        return new InvalidArgumentsException(gql, legacyMessage, cause);
    }

    public static InvalidArgumentsException requiresPositiveIntegerInOptions(String option, int value) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withParam(GqlParams.StringParam.value, String.valueOf(value))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N02)
                        .withParam(GqlParams.StringParam.option, option)
                        .withParam(GqlParams.NumberParam.value, value)
                        .build())
                .build();
        return new InvalidArgumentsException(
                gql, String.format("Option `%s` requires positive integer argument, got `%d`", option, value));
    }

    public static InvalidArgumentsException requiresPositiveIntegerInIds(long value, String expectedFormat) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withParam(GqlParams.StringParam.value, String.valueOf(value))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N02)
                        .withParam(GqlParams.StringParam.option, String.format("id %s", expectedFormat))
                        .withParam(GqlParams.NumberParam.value, value)
                        .build())
                .build();
        return new InvalidArgumentsException(gql, "Negative ids are not supported " + expectedFormat);
    }

    public static InvalidArgumentsException cannotParseId(String value, String expectedFormat) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, value)
                .withParam(GqlParams.StringParam.context, String.format("id %s", expectedFormat))
                .build();
        return new InvalidArgumentsException(gql, "Could not parse id " + expectedFormat);
    }

    public static InvalidArgumentsException cannotParseIdInvalidNumber(
            String value, String idNumber, String expectedFormat, NumberFormatException cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, value)
                .withParam(GqlParams.StringParam.context, String.format("id %s", expectedFormat))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                        .withParam(GqlParams.StringParam.input, idNumber)
                        .withParam(GqlParams.StringParam.context, "a number")
                        .build())
                .build();

        return new InvalidArgumentsException(gql, "Could not parse id " + expectedFormat, cause);
    }

    public static InvalidArgumentsException cannotParseIdInvalidDatabase(
            String value, String dbName, String expectedFormat, IllegalArgumentException cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, value)
                .withParam(GqlParams.StringParam.context, String.format("id %s", expectedFormat))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                        .withParam(GqlParams.StringParam.input, dbName)
                        .withParam(
                                GqlParams.StringParam.context, String.format("database name (%s)", cause.getMessage()))
                        .build())
                .build();
        return new InvalidArgumentsException(gql, cause.getMessage(), cause);
    }

    public static InvalidArgumentsException invalidResource(String typeString) {
        var gqlMsg = String.format("Found invalid resource (%s) in the system graph.", typeString);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), gqlMsg);
        return new InvalidArgumentsException(
                gql, String.format("Found not valid resource (%s) in the system graph.", typeString));
    }

    public static InvalidArgumentsException entityResourceInvalidAction(
            String entity, String action, String legacyFormat) {
        var msg = String.format(
                "%s resource cannot be combined with action %s%s%s", entity, legacyFormat, action, legacyFormat);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), msg);
        return new InvalidArgumentsException(gql, msg);
    }

    public static InvalidArgumentsException internalAlterServer(String name) {
        var msg = String.format("Server '%s' can't be altered: must specify options", name);
        var gql = GqlHelper.get50N00(InvalidArgumentsException.class.getSimpleName(), msg);
        return new InvalidArgumentsException(gql, msg);
    }

    public static InvalidArgumentsException cannotAlterServer(String name, InvalidArgumentsException cause) {
        var msg = String.format("Server '%s' can't be altered: %s", name, cause.getMessage());
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N46)
                        .withParam(GqlParams.StringParam.server, name)
                        .build())
                .withParam(GqlParams.StringParam.msg, "argument error")
                .build();
        return new InvalidArgumentsException(gql, msg, cause);
    }

    public static InvalidArgumentsException cannotEnableServer(String name, InvalidArgumentsException cause) {
        var msg = String.format("Server '%s' can't be enabled: %s", name, cause.getMessage());
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N41)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N48)
                        .withParam(GqlParams.StringParam.server, name)
                        .build())
                .withParam(GqlParams.StringParam.msg, "argument error")
                .build();
        return new InvalidArgumentsException(gql, msg, cause);
    }

    public static InvalidArgumentsException providedFieldEmpty(String field) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                .withParam(GqlParams.StringParam.item, field)
                .build();
        return new InvalidArgumentsException(gql, String.format("The provided %s is empty.", field.toLowerCase()));
    }

    private static ErrorGqlStatusObject getIdxGql(MapValue itemsMap, java.util.List<String> validConfigSettingNames) {
        var prettyVal = new PrettyPrinter();
        itemsMap.writeTo(prettyVal);
        return GqlHelper.getGql42001_22N04(prettyVal.value(), "index config", validConfigSettingNames);
    }

    private static String invalidConfigValueString(PrettyPrinter pp, AnyValue value, String schemaType) {
        value.writeTo(pp);
        return invalidConfigValueString(pp.value(), schemaType);
    }

    private static String invalidConfigValueString(String value, String schemaType) {
        return String.format("Could not create %s with specified index config '%s'", schemaType, value);
    }

    public static InvalidArgumentsException invalidIndexConfigExpectedMap(String schemaType, AnyValue input) {
        var oldMsg = String.format(
                "Could not create %s with specified index config '%s'. Expected a map.", schemaType, input.prettify());
        return invalidInput(input, GqlParams.StringParam.cmd.process("indexConfig"), List.of("MAP"), oldMsg);
    }

    public static InvalidArgumentsException invalidIndexConfig(
            String schemaType, String indexConfigOptions, String indexType) {
        var gql = GqlHelper.getGql42001_22N04(indexConfigOptions, "index config", List.of());
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s with specified index config '%s': %s indexes have no valid config values.",
                        schemaType, indexConfigOptions, indexType));
    }

    public static InvalidArgumentsException invalidIndexConfigExpectedMapWithDoubleArray(
            String schemaType, AnyValue input) {
        var oldMsg = String.format(
                "Could not create %s with specified index config '%s'. Expected a map from String to Double[].",
                schemaType, input.prettify());

        return invalidInput(
                input, GqlParams.StringParam.cmd.process("indexConfig"), List.of("MAP<STRING, LIST<FLOAT>>"), oldMsg);
    }

    public static InvalidArgumentsException invalidIndexConfigExpectedMapToStringAndBoolean(
            String schemaType, AnyValue input) {
        var oldMsg = String.format(
                "Could not create %s with specified index config '%s'. Expected a map from String to Strings and Booleans.",
                schemaType, input.prettify());

        return invalidInput(
                input,
                GqlParams.StringParam.cmd.process("indexConfig"),
                List.of("MAP<STRING, BOOLEAN | STRING>"),
                oldMsg);
    }

    public static InvalidArgumentsException invalidIndexOptionValue(
            String providedOption, List<String> validOptions, String errorMessageOverride) {
        var gql = GqlHelper.getGql42001_22N04(
                providedOption, GqlParams.StringParam.input.process("OPTIONS"), validOptions);
        return new InvalidArgumentsException(
                gql, errorMessageOverride == null ? GqlHelper.getCompleteMessage(gql) : errorMessageOverride);
    }

    public static InvalidArgumentsException invalidIndexProviderSuggestIndex(
            String schemaDescription,
            String providerString,
            String indexDescription,
            String providerIndexType,
            List<String> indexProviders) {
        var indexProvidersString =
                "[" + indexProviders.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", ")) + "]";
        var gql = GqlHelper.getGql42001_22N04(providerString, "index provider", indexProviders);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "To create %s index, please use 'CREATE %s INDEX ...'.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexDescription, providerIndexType, indexProvidersString));
    }

    public static InvalidArgumentsException invalidIndexProvider(String schemaType, AnyValue input) {
        var oldMsg = String.format(
                "Could not create %s with specified index provider '%s'. Expected String value.", schemaType, input);
        return invalidInput(input, GqlParams.StringParam.cmd.process("indexProvider"), List.of("STRING"), oldMsg);
    }

    public static InvalidArgumentsException invalidIndexProvider(
            Boolean correctCypherVersion,
            String schemaDescription,
            String providerString,
            List<String> indexProviders) {
        var indexProvidersString =
                "[" + indexProviders.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", ")) + "]";
        var gql = GqlHelper.getGql42001_22N04(providerString, "index provider", indexProviders);
        var message = correctCypherVersion
                        && (providerString.equalsIgnoreCase("native-btree-1.0")
                                || providerString.equalsIgnoreCase("lucene+native-3.0"))
                ? String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "Invalid index type b-tree, use range, point or text index instead.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexProvidersString)
                : String.format(
                        "Could not create %s with specified index provider '%s'.\n"
                                + "The available index providers for the given type: %s.",
                        schemaDescription, providerString, indexProvidersString);
        return new InvalidArgumentsException(gql, message);
    }

    public static InvalidArgumentsException invalidVectorIndexConfig(String schemaType, AnyValue input) {
        var oldMsg = String.format(
                "Could not create %s with specified index config '%s'. "
                        + "Expected a map from String to Booleans, Strings, Integers and Doubles.",
                schemaType, input.prettify());
        return invalidInput(
                input,
                GqlParams.StringParam.cmd.process("indexConfig"),
                List.of("MAP<STRING, BOOLEAN | STRING | INTEGER | FLOAT>"),
                oldMsg);
    }

    public static InvalidArgumentsException invalidVectorIndexConfigSetting(
            String schemaType, String settingName, String providedValue, String validTypes, String validCypherTypes) {
        var oldMsg = String.format(
                "Could not create %s with specified index config '%s'. Expected %s.",
                schemaType, settingName, validTypes);
        var gql = getGql22G03_22N27(
                providedValue, GqlParams.StringParam.cmd.process(settingName), List.of(validCypherTypes));
        return new InvalidArgumentsException(gql, oldMsg);
    }

    public static InvalidArgumentsException invalidSeedRestoreOption(String operation, String key, AnyValue input) {
        var oldMsg = String.format(
                "Could not %s with specified %s '%s', Integer or datetime expected.", operation, key, input);
        return invalidInput(input, key, List.of("INTEGER", "DATETIME"), oldMsg);
    }

    public static InvalidArgumentsException invalidReplicaConfigOption(
            String operation, String key, String providedOption, List<String> validOptions) {
        var oldMsg = String.format(
                "Could not %s with specified %s option '%s'. Valid options: %s",
                operation, key, providedOption, String.join(",", validOptions));
        var gql = getGql22G03_22N27(providedOption, key, validOptions);
        return new InvalidArgumentsException(gql, oldMsg);
    }

    public static InvalidArgumentsException invalidReplicaConfigOptionType(
            String operation, String key, String subKey, AnyValue input, String expectedType) {
        var oldMsg = String.format(
                "Could not %s with specified %s option %s '%s', %s expected.",
                operation, key, subKey, input, expectedType);
        return invalidInput(input, key, List.of(expectedType), oldMsg);
    }

    public static InvalidArgumentsException missingReplicaConfigForReplicaDatabase() {
        var message = "CREATE REPLICA DATABASE requires 'replicaConfig' in OPTIONS.";
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
                .withParam(GqlParams.ListParam.inputList, List.of("replicaConfig"))
                .build();
        return new InvalidArgumentsException(gql, message);
    }

    public static InvalidArgumentsException invalidStringOption(String operation, String key, AnyValue input) {
        var oldMsg = String.format("Could not %s with specified %s '%s', String expected.", operation, key, input);
        return invalidInput(input, key, List.of("STRING"), oldMsg);
    }

    public static InvalidArgumentsException invalidMapOption(String operation, String key, AnyValue input) {
        var oldMsg = String.format("Could not %s with specified %s '%s', Map expected.", operation, key, input);
        return invalidInput(input, key, List.of("MAP NOT NULL"), oldMsg);
    }

    public static InvalidArgumentsException pointOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains spatial config settings options.\nTo create point index, please use 'CREATE POINT INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException fulltextOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains fulltext config options.\nTo create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException vectorOptionsInConfig(
            PrettyPrinter pp, MapValue itemsMap, String schemaType, java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s, contains vector config options.\nTo create vector index, please use 'CREATE VECTOR INDEX ...'.",
                        invalidConfigValueString(pp, itemsMap, schemaType)));
    }

    public static InvalidArgumentsException unrecognizedIndexConfigSetting(
            PrettyPrinter pp,
            MapValue itemsMap,
            IndexSettingRecord unrecognized,
            String schemaType,
            java.util.List<String> validConfigSettingNames) {
        var gql = getIdxGql(itemsMap, validConfigSettingNames);

        var validValues = "";
        if (!validConfigSettingNames.isEmpty()) {
            var sb = new StringBuilder();
            sb.append(validConfigSettingNames.getFirst());
            for (int i = 1; i < validConfigSettingNames.size(); i++) {
                sb.append(", ").append(validConfigSettingNames.get(i));
            }
            validValues = sb.toString();
        }
        return new InvalidArgumentsException(
                gql,
                java.lang.String.format(
                        "%s. '%s' is an unrecognized setting. Supported: [%s]",
                        invalidConfigValueString(pp, itemsMap, schemaType), unrecognized.settingName(), validValues));
    }

    public static InvalidArgumentsException indexSettingOutOfRange(
            String settingName, String expectedType, String minValue, String maxValue, Object given) {
        var gql = GqlHelper.getGql22003_22N03(settingName, expectedType, minValue, maxValue, stringify(given));
        return new InvalidArgumentsException(
                gql, String.format("'%s' must be between %s and %s inclusively", settingName, minValue, maxValue));
    }

    public static InvalidArgumentsException invalidIndexSettingValue(
            String settingName, List<String> validTypes, Object given) {
        String givenString = stringify(given);
        var supported = "";
        if (!validTypes.isEmpty()) {
            var sb = new StringBuilder();
            sb.append(validTypes.getFirst());
            for (int i = 1; i < validTypes.size(); i++) {
                sb.append(", ").append(validTypes.get(i));
            }
            supported = sb.toString();
        }

        var gql = GqlHelper.getGql42001_22N04(givenString, settingName, validTypes);
        return new InvalidArgumentsException(
                gql,
                String.format("'%s' is an unsupported '%s'. Supported: [%s]", givenString, settingName, supported));
    }

    public static InvalidArgumentsException expectedNonePrimarySecondary(
            String value, String modeConstraint, List<String> expected) {
        var gql = GqlHelper.getGql42001_22N04(value, modeConstraint, expected);
        return new InvalidArgumentsException(
                gql, String.format("%s expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '%s'.", modeConstraint, value));
    }

    public static InvalidArgumentsException unrecognisedOptionGivenValue(
            String operation, String value, String key, String validValue, Boolean formatValidValuesForOld) {
        var validValues = List.of(validValue);
        var gql = GqlHelper.getGql42001_22N04(value, key, validValues);
        if (formatValidValuesForOld) {
            validValue = "'" + validValue + "'";
        }
        return new InvalidArgumentsException(
                gql,
                String.format("Could not %s with specified %s '%s'. Expected %s.", operation, key, value, validValue));
    }

    public static InvalidArgumentsException unrecognisedOptionGivenValue(
            String operation, String key, String value, List<String> validValues) {
        var gql = GqlHelper.getGql42001_22N04(value, key, validValues);
        String validValuesString =
                validValues.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with specified %s '%s', Expected one of %s.",
                        operation, key, value, validValuesString));
    }

    public static InvalidArgumentsException unrecognisedOptionsOnlyKeys(
            String operation, List<String> invalidKeys, List<String> permittedOptions) {
        var permittedOptionsString =
                permittedOptions.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var invalidKeysStringGql = String.join(", ", invalidKeys);
        var invalidKeysStringOld =
                invalidKeys.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var gql = GqlHelper.getGql42001_22N04(
                invalidKeysStringGql, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with unrecognised option(s): %s. Expected %s.",
                        operation, invalidKeysStringOld, permittedOptionsString));
    }

    public static InvalidArgumentsException unrecognisedOptionsNoOperation(
            String invalidKey, List<String> permittedOptions) {
        var permittedOptionsString =
                permittedOptions.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var gql = GqlHelper.getGql42001_22N04(
                invalidKey, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql, String.format("Unrecognised option '%s', expected %s.", invalidKey, permittedOptionsString));
    }

    public static InvalidArgumentsException unrecognisedCreateDbOptions(
            String operation, List<String> invalidKeys, List<String> permittedOptions) {
        var permittedOptionsString =
                permittedOptions.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var invalidKeysStringGql = String.join(", ", invalidKeys);
        var invalidKeysStringOld =
                invalidKeys.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        var gql = GqlHelper.getGql42001_22N04(
                invalidKeysStringGql, GqlParams.StringParam.input.process("OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with 'CREATE DATABASE' option(s): %s. Expected %s.",
                        operation, invalidKeysStringOld, permittedOptionsString));
    }

    public static InvalidArgumentsException invalidCreateDbOptionsKeysCombination(
            String operation, List<String> invalidKeys, List<String> permittedOptions) {
        var permittedOptionsString =
                permittedOptions.stream().map(option -> "'" + option + "'").collect(Collectors.joining(", "));
        // Assumes only 2 keys (and the gql version assumes the parameter will add '...' around it)
        var invalidKeysStringOldMsg =
                invalidKeys.stream().map(key -> "'" + key + "'").collect(Collectors.joining(" and "));
        var invalidKeysString = String.join("' and '", invalidKeys);

        var gql = GqlHelper.getGql42001_22N04(
                invalidKeysString, GqlParams.StringParam.input.process("CREATE DATABASE OPTIONS"), permittedOptions);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Invalid input %s for %s. Expected one of %s.",
                        invalidKeysStringOldMsg, operation, permittedOptionsString));
    }

    public static InvalidArgumentsException unsupportedOptionsKey(String key) {
        var gql = GqlHelper.get51N31(key, "OPTIONS without change_data_capture");
        return new InvalidArgumentsException(gql, String.format("%s is not supported yet", key));
    }

    public static InvalidArgumentsException invalidDriverSettings(
            String operation, String invalidKeys, List<String> validKeys) {
        var validKeysString = String.join(", ", validKeys);
        var gql = GqlHelper.getGql42001_22N04(invalidKeys, GqlParams.StringParam.input.process("DRIVER"), validKeys);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver setting(s) provided: %s. Valid driver settings are: %s",
                        operation, invalidKeys, validKeysString));
    }

    public static InvalidArgumentsException invalidDriverSettingsValue(
            String operation, String key, String expectedType, String expectedCypherType, AnyValue input) {
        var oldMsg = String.format(
                "Failed to %s: Invalid driver settings value for '%s'. Expected %s value.",
                operation, key, expectedType);

        return invalidInput(input, key, List.of(expectedCypherType), oldMsg);
    }

    public static InvalidArgumentsException invalidDriverSettingsExpectedMap(String operation, AnyValue input) {
        var oldMsg =
                String.format("Failed to %s: Invalid driver settings '%s'. Expected a map value.", operation, input);
        return invalidInput(input, GqlParams.StringParam.cmd.process("DRIVER"), List.of("MAP"), oldMsg);
    }

    public static InvalidArgumentsException invalidDriverSettingsExpectedMap42N51(
            String operation, AnyValue input, String paramName) {
        var oldMsg =
                String.format("Failed to %s: Invalid driver settings '%s'. Expected a map value.", operation, input);
        return invalidInput42N51(input, GqlParams.StringParam.cmd.process("DRIVER"), List.of("MAP"), oldMsg, paramName);
    }

    public static InvalidArgumentsException invalidPropertiesExpectedMap(String operation, AnyValue input) {
        var oldMsg = String.format("Failed to %s: Invalid properties '%s'. Expected a map value.", operation, input);
        return invalidInput(input, GqlParams.StringParam.cmd.process("PROPERTIES"), List.of("MAP"), oldMsg);
    }

    public static InvalidArgumentsException invalidPropertiesExpectedMap42N51(
            String operation, AnyValue input, String paramName) {
        var oldMsg = String.format("Failed to %s: Invalid properties '%s'. Expected a map value.", operation, input);
        return invalidInput42N51(
                input, GqlParams.StringParam.cmd.process("PROPERTIES"), List.of("MAP"), oldMsg, paramName);
    }

    public static InvalidArgumentsException unexpectedDriverSettingValue(
            String operation, String value, String settingKey, List<String> validValues) {
        var validValuesString = String.join(", ", validValues);
        var gql = GqlHelper.getGql42001_22N04(value, GqlParams.StringParam.input.process("DRIVER"), validValues);
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver settings value for '%s'. Expected one of %s.",
                        operation, settingKey, validValuesString));
    }

    public static InvalidArgumentsException invalidOptionFormat(
            String operation, String key, String value, List<String> validFormats) {
        var gql = GqlHelper.getGql42001_22N04(value, key, validFormats);
        var validFormatsString =
                validFormats.stream().map(format -> "'" + format + "'").collect(Collectors.joining(", "));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not %s with specified %s '%s'. Unknown format, supported formats are %s",
                        operation, key, value, validFormatsString));
    }

    public static InvalidArgumentsException missingOptionCreateSchema(
            String schemaType, List<String> option, String quotedOptions) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
                .withParam(GqlParams.ListParam.inputList, option)
                .build();
        return new InvalidArgumentsException(
                gql, String.format("Failed to create %s: Missing index config options %s.", schemaType, quotedOptions));
    }

    public static InvalidArgumentsException invalidOptionsExpectedMap(String operation, AnyValue input) {
        var oldMsg = String.format("Could not %s with options '%s'. Expected a map value.", operation, input);
        return invalidInput(input, GqlParams.StringParam.cmd.process("OPTIONS"), List.of("MAP"), oldMsg);
    }

    public static InvalidArgumentsException compositeUsingOptions(String operation) {
        var gql = GqlHelper.getGql22N81(
                GqlParams.StringParam.cmd.process("OPTIONS"),
                GqlParams.StringParam.cmd.process("CREATE COMPOSITE DATABASE"));
        return new InvalidArgumentsException(
                gql, String.format("Could not %s: composite databases have no valid options values.", operation));
    }

    public static InvalidArgumentsException noValidPropertyConstraintOptions(String entity, String constraintType) {
        var gql = GqlHelper.getGql22N81(
                GqlParams.StringParam.cmd.process("OPTIONS"),
                String.format("%s property %s constraints", entity, constraintType));
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Could not create %s property %s constraint: property %s constraints have no valid options values.",
                        entity, constraintType, constraintType));
    }

    public static InvalidArgumentsException invalidArgumentRoleHasAuthRule(String subquery, String parameter) {
        var rootCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22ND1)
                .withParam(GqlParams.StringParam.query, subquery);
        var gql = GqlHelper.invalidSyntax()
                .withCause(GqlHelper.maybeInvalidParameter(parameter, rootCause).build())
                .build();
        return new InvalidArgumentsException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentsException invalidArgumentRoleHasDeniedPrivileges(String subquery, String parameter) {
        var rootCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22ND2)
                .withParam(GqlParams.StringParam.query, subquery);
        var gql = GqlHelper.invalidSyntax()
                .withCause(maybeInvalidParameter(parameter, rootCause).build())
                .build();
        return new InvalidArgumentsException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static InvalidArgumentsException connectionPoolSizeZeroNotAllowed(String operation, String pool_max_size) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, "0")
                .withParam(GqlParams.StringParam.context, operation)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N86)
                        .build())
                .build();
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to %s: Invalid driver settings value for '%s'. Zero is not allowed.",
                        operation, pool_max_size));
    }

    public static InvalidArgumentsException optionRequiresInteger(String option, Object value) {
        var gql = getGql22G03_22N27(option, value.getClass().getTypeName(), List.of("INTEGER"));
        return new InvalidArgumentsException(
                gql, String.format("Option `%s` requires integer argument, got `%s`", option, value));
    }

    public static InvalidArgumentsException inputContainsInvalidCharacters(
            String invalidInput, String context, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, invalidInput)
                .withParam(GqlParams.StringParam.context, context)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
                        .withParam(GqlParams.StringParam.input, invalidInput)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
        return new InvalidArgumentsException(gql, legacyMessage);
    }

    public static InvalidArgumentsException failedEvaluatingDriverSettings(Exception e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N89)
                .withParam(GqlParams.StringParam.cause, e.getMessage())
                .build();
        return new InvalidArgumentsException(gql, "Failed evaluating the given driver settings.", e);
    }

    public static InvalidArgumentsException cannotAlterImmutableCompositeDb(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N90)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new InvalidArgumentsException(
                gql,
                String.format(
                        "Failed to alter the specified database '%s': Composite databases cannot be altered.", dbName));
    }

    public static InvalidArgumentsException dbNameIsNotWellDefined(AnyValue input) {
        var oldMsg = "Could not create database - name is not well defined";
        return invalidInput(input, "database name", List.of("STRING"), oldMsg);
    }

    public static InvalidArgumentsException expectedListOfTags(String key, AnyValue input, String listString) {
        var oldMsg = String.format("%s expects a list of tags but got '%s'.", key, listString);
        return invalidInput(input, key, List.of("LIST<STRING>"), oldMsg);
    }

    public static InvalidArgumentsException expectedListOfTagsNames(String key, AnyValue input) {
        var oldMsg = String.format("%s expects a list of tags names but got '%s'.", key, input);
        return invalidInput(input, key, List.of("LIST<STRING>"), oldMsg);
    }

    public static InvalidArgumentsException expectedListOfDatabaseNames(String action, String list, AnyValue input) {
        var oldMsg = String.format("%s expects a list of database names but got '%s'.", action, list);
        return invalidInput(input, action, List.of("LIST<STRING>"), oldMsg);
    }

    public static InvalidArgumentsException driverSettingDurationNotPositive(
            String operation, String key, DurationValue duration) {
        var legacyMessage = "Failed to %s: Invalid driver settings value for '%s'. Negative duration is not allowed."
                .formatted(operation, key);
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withParam(GqlParams.StringParam.value, duration.prettyPrint())
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                        .withParam(GqlParams.StringParam.component, key)
                        .withParam(GqlParams.StringParam.valueType, "DURATION")
                        .withParam(GqlParams.StringParam.lower, DurationValue.ZERO.prettyPrint())
                        .withParam(GqlParams.StringParam.upper, DurationValue.MAX_VALUE.prettyPrint())
                        .withParam(GqlParams.StringParam.value, duration.prettyPrint())
                        .build())
                .build();
        return new InvalidArgumentsException(gql, legacyMessage);
    }

    private static InvalidArgumentsException invalidInput(
            AnyValue input, String context, List<String> validTypes, String oldMessage) {
        var gql = GqlHelper.getGql22G03_22N27(input.prettify(), context, validTypes);
        return new InvalidArgumentsException(gql, oldMessage);
    }

    private static InvalidArgumentsException invalidInput42N51(
            AnyValue input, String context, List<String> validTypes, String oldMessage, String paramName) {
        var cause = GqlHelper.getGql22G03_22N27(input.prettify(), context, validTypes);
        var gql = getGql42N51(paramName, cause);
        return new InvalidArgumentsException(gql, oldMessage);
    }
}
