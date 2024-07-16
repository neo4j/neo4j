/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.common.ast.factory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ASTExceptionFactory {
    Exception syntaxException(String got, List<String> expected, Exception source, int offset, int line, int column);

    Exception syntaxException(Exception source, int offset, int line, int column);

    // Exception messages
    String invalidDropCommand = "Unsupported drop constraint command: Please delete the constraint by name instead";

    static String relationshipPatternNotAllowed(ConstraintType type) {
        return String.format("'%s' does not allow relationship patterns", type.description());
    }

    static String nodePatternNotAllowed(ConstraintType type) {
        return String.format("'%s' does not allow node patterns", type.description());
    }

    static String onlySinglePropertyAllowed(ConstraintType type) {
        return String.format("Constraint type '%s' does not allow multiple properties", type.description());
    }

    static String invalidShowFilterType(String command, ShowCommandFilterTypes got) {
        return String.format("Filter type %s is not defined for show %s command.", got.description(), command);
    }

    String invalidExistsForShowConstraints =
            "`SHOW CONSTRAINTS` no longer allows the `EXISTS` keyword, please use `EXIST` or `PROPERTY EXISTENCE` instead.";

    static String invalidBriefVerbose(String command) {
        return String.format(
                """
`%s` no longer allows the `BRIEF` and `VERBOSE` keywords,
please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`.""",
                command);
    }

    static String invalidCreateIndexType(CreateIndexTypes got) {
        return String.format("Index type %s is not defined for create index command.", got.description());
    }

    // gives back error message if the command is invalid, null if valid
    static String checkForInvalidCreateConstraint(
            ConstraintType type, ConstraintVersion constraintVersion, Boolean containsOn, Boolean moreThanOneProperty) {
        // Error messages for mixing old and new constraint syntax
        String errorMessageOnRequire =
                "Invalid constraint syntax, ON should not be used in combination with REQUIRE. Replace ON with FOR.";
        String errorMessageForAssert =
                "Invalid constraint syntax, FOR should not be used in combination with ASSERT. Replace ASSERT with REQUIRE.";
        String errorMessageForAssertExists =
                "Invalid constraint syntax, FOR should not be used in combination with ASSERT EXISTS. Replace ASSERT EXISTS with REQUIRE ... IS NOT NULL.";
        String errorMessageOnAssert =
                "Invalid constraint syntax, ON and ASSERT should not be used. Replace ON with FOR and ASSERT with REQUIRE.";
        String errorMessageOnAssertExists =
                "Invalid constraint syntax, ON and ASSERT EXISTS should not be used. Replace ON with FOR and ASSERT EXISTS with REQUIRE ... IS NOT NULL.";

        String message = null;
        if (type == ConstraintType.NODE_EXISTS
                || type == ConstraintType.NODE_IS_NOT_NULL
                || type == ConstraintType.REL_EXISTS
                || type == ConstraintType.REL_IS_NOT_NULL) {
            // existence constraints
            if (moreThanOneProperty && (type == ConstraintType.NODE_EXISTS || type == ConstraintType.REL_EXISTS)) {
                // previously thrown during ast construction so need to check this first to keep order of checks
                message = onlySinglePropertyAllowed(type);
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_2 && containsOn) {
                message = errorMessageOnRequire; // ON ... REQUIRE ... IS NOT NULL
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_1 && !containsOn) {
                message = errorMessageForAssert; // FOR ... ASSERT ... IS NOT NULL
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_0 && !containsOn) {
                message = errorMessageForAssertExists; // FOR ... ASSERT EXISTS ...
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_1) {
                message = errorMessageOnAssert; // ON ... ASSERT ... IS NOT NULL
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_0) {
                message = errorMessageOnAssertExists; // ON ... ASSERT EXISTS ...
            }
        } else {
            // remaining constraints
            if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_2 && containsOn) {
                message = errorMessageOnRequire; // ON ... REQUIRE
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_0 && !containsOn) {
                message = errorMessageForAssert; // FOR ... ASSERT
            } else if (constraintVersion == ConstraintVersion.CONSTRAINT_VERSION_0) {
                message = errorMessageOnAssert; // ON ... ASSERT
            }
        }
        return message;
    }

    static String invalidDropConstraint(ConstraintType type, Boolean moreThanOneProperty) {
        String messageFormat =
                "%s constraints cannot be dropped by schema, please drop by name instead: DROP CONSTRAINT constraint_name. The constraint name can be found using SHOW CONSTRAINTS.";
        String message;
        switch (type) {
            case NODE_UNIQUE:
                message = String.format(messageFormat, "Uniqueness");
                break;
            case NODE_KEY:
                message = String.format(messageFormat, "Node key");
                break;
            case NODE_EXISTS:
                if (moreThanOneProperty) {
                    message = onlySinglePropertyAllowed(type);
                } else {
                    message = String.format(messageFormat, "Node property existence");
                }
                break;
            case REL_EXISTS:
                if (moreThanOneProperty) {
                    message = onlySinglePropertyAllowed(type);
                } else {
                    message = String.format(messageFormat, "Relationship property existence");
                }
                break;
            default:
                // ConstraintType.NODE_IS_NOT_NULL, ConstraintType.REL_IS_NOT_NULL,
                // ConstraintType.REL_UNIQUE, ConstraintType.REL_KEY
                message = invalidDropCommand;
        }
        return message;
    }

    static String invalidDotsInRemoteAliasName(String name) {
        return String.format(
                "'.' is not a valid character in the remote alias name '%s'. "
                        + "Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
                name);
    }

    static String tooManyAliasNameComponents(String name) {
        return String.format(
                "Invalid input `%s` for name. Expected name to contain at most two components separated by `.`.", name);
    }

    static String tooManyDatabaseNameComponents(String name) {
        return String.format(
                "Invalid input `%s` for database name. Expected name to contain at most one component.", name);
    }

    String periodicCommitNotSupported =
            "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead.";

    String namedPatternInInsertNotSupported =
            "Named patterns are not allowed in `INSERT`. Use `CREATE` instead or remove the name.";

    String colonConjunctionInInsertNotSupported =
            "Colon `:` conjunction is not allowed in INSERT. Use `CREATE` or conjunction with ampersand `&` instead.";

    static String invalidHintIndexType(HintIndexType got) {
        final String HINT_TYPES = Arrays.stream(HintIndexType.values())
                .filter(hintIndexType -> !(hintIndexType == HintIndexType.BTREE || hintIndexType == HintIndexType.ANY))
                .map(Enum::name)
                .collect(Collectors.collectingAndThen(Collectors.toList(), joiningLastDelimiter(", ", " or ")));
        if (got == HintIndexType.BTREE) {
            return String.format(
                    "Index type %s is no longer supported for USING index hint. Use %s instead.",
                    got.name(), HINT_TYPES);
        } else {
            return String.format(
                    "Index type %s is not defined for USING index hint. Use %s instead.", got.name(), HINT_TYPES);
        }
    }

    String failedToParseFile =
            "Failed to parse the file expression. Please remember to use quotes for string literals.";

    // ---------Helper functions
    private static Function<List<String>, String> joiningLastDelimiter(String delimiter, String lastDelimiter) {
        return list -> {
            int last = list.size() - 1;
            return String.join(lastDelimiter, String.join(delimiter, list.subList(0, last)), list.get(last));
        };
    }
}
