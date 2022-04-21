/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.parser;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Optional;

/**
 * An object capable of parsing a piece of text and returning a List statements contained within.
 */
public interface StatementParser {
    /**
     * Consumes and parses all statements of the specified reader, including incomplete statements.
     */
    ParsedStatements parse(Reader reader) throws IOException;

    /**
     * Consumes and parses all statements of the specified reader, including incomplete statements.
     */
    ParsedStatements parse(String line) throws IOException;

    record ParsedStatements(List<ParsedStatement> statements) {
        public boolean isEmpty() {
            return statements.isEmpty();
        }

        public boolean hasIncompleteStatement() {
            return statements.stream().anyMatch(statement -> statement instanceof CypherStatement c && !c.isComplete());
        }

        public Optional<ParsedStatement> statementAtOffset(int offset) {
            for (int i = statements.size() - 1; i >= 0; --i) {
                var statement = statements.get(i);
                if ((!statement.isComplete() && offset >= statement.beginOffset())
                        || (offset >= statement.beginOffset() && offset <= statement.endOffset())) {
                    return Optional.of(statement);
                }
            }
            return Optional.empty();
        }
    }

    sealed interface ParsedStatement permits CommandStatement, CypherStatement {
        String statement();

        int beginOffset();

        int endOffset();

        boolean isComplete();
    }

    record CommandStatement(String name, List<String> args, boolean isComplete, int beginOffset, int endOffset)
            implements ParsedStatement {
        @Override
        public String statement() {
            return name + " " + String.join(" ", args);
        }
    }

    record CypherStatement(String statement, boolean isComplete, int beginOffset, int endOffset)
            implements ParsedStatement {
        public static CypherStatement complete(String statement) {
            return new CypherStatement(statement, true, 0, statement.length() == 0 ? 0 : statement.length() - 1);
        }
    }
}
