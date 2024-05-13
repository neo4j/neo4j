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
package org.neo4j.cypher.internal.ast.factory;

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

    static String invalidCreateIndexType(CreateIndexTypes got) {
        return String.format("Index type %s is not defined for create index command.", got.description());
    }

    static String invalidDotsInRemoteAliasName(String name) {
        return String.format(
                "'.' is not a valid character in the remote alias name '%s'. "
                        + "Remote alias names using '.' must be quoted with backticks e.g. `remote.alias`.",
                name);
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
