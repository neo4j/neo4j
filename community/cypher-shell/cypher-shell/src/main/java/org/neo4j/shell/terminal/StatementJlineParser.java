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

import static org.jline.reader.Parser.ParseContext.COMPLETE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.jline.reader.CompletingParsedLine;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.CypherLanguageService;
import org.neo4j.shell.parser.CypherLanguageService.Token;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.terminal.JlineTerminal.ParsedLineStatements;

/**
 * Jline Parser that parse Cypher Shell statements.
 */
public class StatementJlineParser extends DefaultParser implements Parser {
    private static final Logger log = Logger.create();
    private final StatementParser statementParser;
    private final CypherLanguageService cypherSyntax;
    private boolean enableStatementParsing;

    public StatementJlineParser(StatementParser statementParser, CypherLanguageService cypherSyntax) {
        this.statementParser = statementParser;
        this.cypherSyntax = cypherSyntax;
    }

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) throws SyntaxError {
        if (!enableStatementParsing) {
            return new UnparsedLine(line, cursor);
        } else if (context == COMPLETE) {
            return parseForCompletion(line, cursor);
        }

        return parseForExecution(line, cursor);
    }

    private SimpleParsedStatements parseForExecution(String line, int cursor) {
        var parsed = parse(line);

        if (parsed.hasIncompleteStatement()) {
            // This will trigger line continuation in jline
            throw new EOFError(-1, cursor, "Incomplete statement");
        }

        return new SimpleParsedStatements(parsed, line, cursor);
    }

    private ParsedLine parseForCompletion(String line, int cursor) {
        return parse(line)
                .statementAtOffset(cursor)
                .flatMap(s -> completingStatement(s, line, cursor))
                .orElseGet(() -> new BlankCompletion(line, cursor));
    }

    private Optional<ParsedLine> completingStatement(ParsedStatement statement, String line, int cursor) {
        if (statement instanceof CommandStatement command && command.args().isEmpty()) {
            return Optional.of(new CommandCompletion(command, line, cursor));
        } else if (statement instanceof CypherStatement cypher) {
            return Optional.of(completingCypher(cypher, line, cursor));
        }

        return Optional.empty();
    }

    private CypherCompletion completingCypher(CypherStatement statement, String line, int cursor) {
        var tokens = cypherSyntax.tokenize(statement.statement());
        int statementStart = statement.beginOffset();

        for (var token : tokens) {
            var tokenStart = statementStart + token.beginOffset();
            var tokenEnd = statementStart + token.endOffset();

            if (cursor >= tokenStart && cursor - 1 <= tokenEnd) {
                // Note, we can't use token.image because it's not always identical how it appears in the query string
                var word = line.substring(tokenStart, tokenEnd + 1);
                return new CypherCompletion(statement, line, cursor, tokens, word, cursor - tokenStart);
            }
        }

        // Found no token at the cursor position
        return new CypherCompletion(statement, line, cursor, tokens, "", 0);
    }

    private ParsedStatements parse(String line) {
        try {
            return statementParser.parse(line);
        } catch (IOException e) {
            log.error("Failed to parse " + line, e);
            throw new RuntimeException("Failed to parse `" + line + "`: " + e.getMessage(), e);
        }
    }

    /** If enable is false this parser will be disabled and pass through all lines without parsing and completion */
    public void setEnableStatementParsing(boolean enable) {
        this.enableStatementParsing = enable;
    }

    @Override
    public boolean isEscapeChar(char ch) {
        return false;
    }

    @Override
    public boolean validCommandName(String name) {
        return false;
    }

    @Override
    public boolean validVariableName(String name) {
        return false;
    }

    @Override
    public String getCommand(String line) {
        return "";
    }

    @Override
    public String getVariable(String line) {
        return null;
    }

    protected record CommandCompletion(CommandStatement statement, String line, int cursor)
            implements CompletingWord, CompletingStatements {
        @Override
        public String word() {
            return statement.name();
        }

        @Override
        public int wordCursor() {
            return 0;
        }
    }

    protected record BlankCompletion(String line, int cursor) implements NoWordsParsedLine {}

    protected record CypherCompletion(
            CypherStatement statement, String line, int cursor, List<Token> tokens, String word, int wordCursor)
            implements CompletingWord, CompletingStatements {}

    protected record SimpleParsedStatements(ParsedStatements statements, String line, int cursor)
            implements ParsedLineStatements, NoWordsParsedLine {}

    protected record UnparsedLine(String line, int cursor) implements NoWordsParsedLine {}

    interface CompletingStatements {
        ParsedStatement statement();
    }

    protected interface NoWordsParsedLine extends CompletingWord {
        @Override
        default String word() {
            return "";
        }

        @Override
        default int wordCursor() {
            return 0;
        }
    }

    private interface CompletingWord extends CompletingParsedLine {
        @Override
        default CharSequence escape(CharSequence candidate, boolean complete) {
            return candidate;
        }

        @Override
        default int rawWordCursor() {
            return wordCursor();
        }

        @Override
        default int rawWordLength() {
            return word().length();
        }

        @Override
        default int wordIndex() {
            return 0;
        }

        @Override
        default List<String> words() {
            return Collections.singletonList(word());
        }
    }
}
