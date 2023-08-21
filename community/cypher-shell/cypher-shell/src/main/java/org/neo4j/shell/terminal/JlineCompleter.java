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
package org.neo4j.shell.terminal;

import java.util.List;
import java.util.stream.Stream;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.neo4j.shell.commands.Command;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.CypherLanguageService;
import org.neo4j.shell.terminal.StatementJlineParser.BlankCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CommandCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CypherCompletion;

/**
 * Provides autocompletion for cypher shell statements.
 */
public class JlineCompleter implements Completer {
    private final CommandCompleter commandCompleter;
    private final CypherCompleter cypherCompleter;
    private final boolean enableCypherCompletion;

    public JlineCompleter(
            CommandFactoryHelper commands,
            CypherLanguageService parser,
            ParameterService parameters,
            boolean enableCypherCompletion) {
        this.commandCompleter = CommandCompleter.from(commands);
        this.cypherCompleter = new CypherCompleter(parser, parameters);
        this.enableCypherCompletion = enableCypherCompletion;
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        // Note, the JavaCC parser doesn't provide good enough completion for
        // us to release it yet. For this reason, cypher completion is
        // disabled by default for now until we decide exactly where we want to
        // go with this.
        try {
            if (line instanceof BlankCompletion) {
                candidates.addAll(commandCompleter.complete());
                if (enableCypherCompletion) {
                    cypherCompleter.completeBlank().forEach(candidates::add);
                }
            } else if (line instanceof CommandCompletion) {
                candidates.addAll(commandCompleter.complete());
            } else if (enableCypherCompletion && line instanceof CypherCompletion cypher) {
                cypherCompleter.complete(cypher).forEach(candidates::add);
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    private record CommandCompleter(List<Suggestion> allCommands) {
        List<Suggestion> complete() {
            return allCommands;
        }

        public static CommandCompleter from(CommandFactoryHelper commands) {
            return new CommandCompleter(
                    commands.metadata().map(Suggestion::command).toList());
        }
    }

    private record CypherCompleter(CypherLanguageService parser, ParameterService parameterMap) {
        Stream<Suggestion> complete(CypherCompletion cypher) {
            return concat(identifiers(cypher), parameters(), keywords(queryUntilCompletionWord(cypher)));
        }

        Stream<Suggestion> completeBlank() {
            return concat(keywords(""), parameters());
        }

        /*
         * Returns cypher keyword suggestions, for example `MATCH`.
         */
        private Stream<Suggestion> keywords(String query) {
            var suggested = parser.suggestNextKeyword(query);

            if (suggested.isEmpty()) {
                return parser.keywords().map(Suggestion::cypher);
            }

            return suggested.stream().map(Suggestion::cypher);
        }

        /*
         * Returns identifier suggestions, for example `myNode` if query is `match (myNode)`.
         */
        private Stream<Suggestion> identifiers(CypherCompletion cypher) {
            return cypher.tokens().stream()
                    .filter(t -> t.isIdentifier() && !t.isParameterIdentifier())
                    // Remove the incomplete statement at the end that we're trying to auto-complete
                    .filter(i -> i.endOffset() + cypher.statement().beginOffset()
                            != cypher.statement().endOffset())
                    .map(t -> Suggestion.identifier(t.image()));
        }

        /*
         * Returns query parameter suggestions, for example `$myParameter`.
         */
        private Stream<Suggestion> parameters() {
            return parameterMap.parameters().keySet().stream().map(Suggestion::parameter);
        }

        private String queryUntilCompletionWord(CypherCompletion cypher) {
            int cutAt =
                    cypher.cursor() - cypher.wordCursor() - cypher.statement().beginOffset();
            return cypher.statement().statement().substring(0, cutAt);
        }

        @SafeVarargs
        private static <T> Stream<T> concat(Stream<T>... streams) {
            return Stream.of(streams).reduce(Stream::concat).orElseGet(Stream::empty);
        }
    }

    private enum SuggestionGroup {
        COMMAND("Commands"),
        IDENTIFIER("Query Identifiers"),
        PARAMETER("Query Parameters"),
        CYPHER("Query Syntax Suggestions (not complete)");

        private final String groupName;

        SuggestionGroup(String groupName) {
            this.groupName = groupName;
        }
    }

    private static class Suggestion extends Candidate {
        Suggestion(String value, SuggestionGroup group, String desc, boolean complete) {
            super(value, value, group.groupName, desc, null, null, complete);
        }

        public static Suggestion cypher(String cypher) {
            return new Suggestion(cypher, SuggestionGroup.CYPHER, null, true);
        }

        public static Suggestion command(Command.Metadata command) {
            return new Suggestion(command.name(), SuggestionGroup.COMMAND, command.description(), true);
        }

        public static Suggestion identifier(String identifier) {
            return new Suggestion(identifier, SuggestionGroup.IDENTIFIER, null, false);
        }

        public static Suggestion parameter(String parameterName) {
            return new Suggestion("$" + parameterName, SuggestionGroup.PARAMETER, null, true);
        }
    }
}
