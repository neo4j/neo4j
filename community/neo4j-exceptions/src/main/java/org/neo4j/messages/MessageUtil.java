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
package org.neo4j.messages;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;

/**
 * Collection of error and log messages (usable in product and test classes)
 */
public class MessageUtil {
    public enum Numerus {
        NONE, //  = 0 entities
        SINGULAR, //  = 1 entity
        PLURAL; // >= 2 entities

        public static Numerus of(int number) {
            if (number < 0) {
                throw new IllegalArgumentException("Numerus of " + number + " is not defined.");
            }
            return switch (number) {
                case 0 -> NONE;
                case 1 -> SINGULAR;
                default -> PLURAL;
            };
        }
    }

    // authentication
    private static final String CREATE_NODE_WITH_LABELS_DENIED =
            "Create node with labels '%s' on database '%s' is not allowed for %s.";
    private static final String WITH_USER = "user '%s' with %s";
    private static final String OVERRIDDEN_MODE = "%s overridden by %s";
    private static final String RESTRICTED_MODE = "%s restricted to %s";

    // alias
    private static final String ALTER_TO_REMOTE =
            "Failed to alter the specified database alias '%s': alter a local alias to a remote alias is not supported.";

    // security
    private static final String FAILED_TO_READ_ENCRYPTION_KEY =
            "Failed to read the symmetric key from the configured keystore";

    // hints
    // 1 = operatorDescription, 2 = hint serialization, 3 = details
    private static final String HINT_ERROR = "Cannot use %1$s hint `%2$s` in this context: %3$s";
    // 1 = missingThingDescription, 2 = foundThingDescription, 3 = entityDescription, 4 = entityName, 5 = additionalInfo
    private static final String HINT_MISSING_PROPERTY_LABEL_DETAIL =
            "Must use %1$s, that the hint is referring to, on %3$s either in the pattern"
                    + " or in supported predicates in `WHERE` (either directly or as part of a top-level `AND` or `OR`), but %2$s found."
                    + " %5$s"
                    + " Note that %1$s must be specified on a non-optional %4$s.";
    private static final String HINT_TEXT_INDEX_DETAIL = "The hint specifies using a text index but %s";

    /**
     * authentication & authorization messages
     */
    public static String createNodeWithLabelsDenied(String labels, String database, String user) {
        return String.format(CREATE_NODE_WITH_LABELS_DENIED, labels, database, user);
    }

    // security context
    public static String authDisabled(String mode) {
        return "AUTH_DISABLED with " + mode;
    }

    // username description
    public static String withUser(String user, String mode) {
        return String.format(WITH_USER, user, mode);
    }

    // mode names
    public static String overriddenMode(String original, String wrapping) {
        return String.format(OVERRIDDEN_MODE, original, wrapping);
    }

    public static String restrictedMode(String original, String wrapping) {
        return String.format(RESTRICTED_MODE, original, wrapping);
    }

    public static String standardMode(Set<String> roles) {
        Set<String> sortedRoles = new TreeSet<>(roles);
        return roles.isEmpty() ? "no roles" : "roles " + sortedRoles;
    }

    /**
     * alias messages
     */
    public static String alterToLocalAlias(String alias) {
        return String.format(ALTER_TO_REMOTE, alias);
    }

    /**
     * Security messages
     */
    public static String failedToFindEncryptionKeyInKeystore(String keyName) {
        return String.format(
                "%s. The key '%s' was not found in the given keystore file.", FAILED_TO_READ_ENCRYPTION_KEY, keyName);
    }

    public static String failedToReadEncryptionKey(String... settings) {
        return String.format(
                "%s. Please verify the keystore configurations: %s.",
                FAILED_TO_READ_ENCRYPTION_KEY, StringUtils.join(settings, ", "));
    }

    public static String failedToEncryptPassword() {
        return "Failed to encrypt remote user password.";
    }

    public static String failedToDecryptPassword() {
        return "Failed to decrypt remote user password.";
    }

    public static String invalidScheme(String url, List<String> schemes) {
        return String.format(
                "The provided url '%s' has an invalid scheme. Please use one of the following schemes: %s.",
                url, StringUtils.join(schemes, ", "));
    }

    public static String insecureScheme(String url, List<String> schemes) {
        return String.format(
                "The provided url '%s' is not a secure scheme. Please use one of the following schemes: %s.",
                url, StringUtils.join(schemes, ", "));
    }

    // hints
    public static String createHintError(String operatorDescription, String hintSerialization, String details) {
        return String.format(HINT_ERROR, operatorDescription, hintSerialization, details);
    }

    public static String createTextIndexHintError(String hintSerialization, Numerus foundPredicates) {
        var predicatesString =
                switch (foundPredicates) {
                    case NONE ->
                    // this should be caught semantic checking but let's provide a meaningful error message anyways
                    "no matching predicate was found.";
                    case SINGULAR -> "the predicate found cannot be used by a text index.";
                    case PLURAL -> "none of the predicates found can be used by a text index.";
                };
        String suggestion;
        if (foundPredicates != Numerus.NONE) {
            suggestion =
                    " You could try to convert the compared value to string by calling `toString()` on it or by testing whether it is a string using `STARTS WITH`.";
        } else {
            suggestion = "";
        }
        String documentation =
                " For more information on when a text index is applicable, please consult the documentation on the use of text indexes.";
        return createHintError(
                "text index",
                hintSerialization,
                String.format(HINT_TEXT_INDEX_DETAIL, predicatesString + suggestion + documentation));
    }

    public static String createMissingPropertyLabelHintError(
            String operatorDescription,
            String hintSerialization,
            String missingThingDescription,
            String foundThingsDescription,
            String entityDescription,
            String entityName,
            String additionalInfo) {
        return createHintError(
                operatorDescription,
                hintSerialization,
                String.format(
                        HINT_MISSING_PROPERTY_LABEL_DETAIL,
                        missingThingDescription,
                        foundThingsDescription,
                        entityDescription,
                        entityName,
                        additionalInfo));
    }

    private static final String SELF_REFERENCE_TO_VARIABLE_WITH_UNKNOWN_TYPE_IN_CREATE_PATTERN_ERROR =
            "The variable '%1$s' is referencing an entity that is created in the same %2$s clause which is not allowed. "
                    + "Please only reference variables created in earlier clauses.";

    private static final String SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN_ERROR =
            "The %1$s variable '%2$s' is referencing a %1$s that is created in the same %3$s clause which is not allowed. "
                    + "Please only reference variables created in earlier clauses.";

    public static String createSelfReferenceError(String name, String clauseName) {
        return String.format(SELF_REFERENCE_TO_VARIABLE_WITH_UNKNOWN_TYPE_IN_CREATE_PATTERN_ERROR, name, clauseName);
    }

    public static String createSelfReferenceError(String name, String variableType, String clauseName) {
        return String.format(SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN_ERROR, variableType, name, clauseName);
    }
}
