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
package org.neo4j.shell.parser;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.ESCAPED_SYMBOLIC_NAME;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER;
import static org.neo4j.cypher.internal.parser.javacc.IdentifierTokens.getIdentifierTokens;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory;
import org.neo4j.cypher.internal.ast.factory.empty.NullAstFactory;
import org.neo4j.cypher.internal.literal.interpreter.LiteralInterpreter;
import org.neo4j.cypher.internal.parser.javacc.Cypher;
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream;
import org.neo4j.cypher.internal.parser.javacc.CypherConstants;
import org.neo4j.cypher.internal.parser.javacc.ParseException;

/**
 * Cypher language service backed by the official Java CC Cypher parser.
 *
 * This implementation has some known caveats:
 *
 * - The tokenizer is un-intuitive in some regards (see comment on {@link #tokenize(String)}).
 * - Keyword suggestions are not always correct (see comment on {@link #suggestNextKeyword(String)}.
 */
public class JavaCcCypherLanguageService implements CypherLanguageService {

    /**
     * Returns tokenized query or empty list if tokenization fails.
     *
     * Caveats:
     *
     * - Comments are not tokenized (they are in JavaCC, as "special token", but ignored here.
     * - Identifiers can be incorrectly tokenized, for example in `match (match) return match;`
     */
    @Override
    public List<Token> tokenize(String query) {
        if (query.isEmpty()) {
            return List.of();
        }

        try {
            var result = new ArrayList<Token>();
            new TokenIterator(query).forEachRemaining(result::add);
            return result;
        } catch (RuntimeException e) {
            // We don't trust the tokenizer to handle all possible input, it fails on empty strings for example.
            return List.of();
        }
    }

    @Override
    public Stream<String> keywords() {
        return collectKeywords(getIdentifierTokens().stream());
    }

    /**
     * Returns suggestions for the next keyword based on the specified incomplete query.
     *
     * This implementation sometimes returns suggestions that would lead to an invalid query.
     */
    // TODO This implementation is a hack.
    @Override
    public List<String> suggestNextKeyword(String incompleteQuery) {
        var tokens = suggestNextToken(incompleteQuery, incompleteQuery.length());
        if (tokens.isEmpty()) {
            // If query parsed successfully we get no suggestions, try adding something wrong at the end ¯\_(ツ)_/¯.
            tokens = suggestNextToken(incompleteQuery + " CYPHER", incompleteQuery.length());
        }

        return collectKeywords(tokens.stream().filter(this::isIdentifierToken)).toList();
    }

    private Set<Integer> suggestNextToken(String query, int nextOffset) {
        try {
            @SuppressWarnings({"rawtypes", "unchecked"})
            var cypher = new Cypher(new NullAstFactory(), new SimpleAstExceptionFactory(), new CypherCharStream(query));
            cypher.Statements();
        } catch (ParseSyntaxException e) {
            // Error needs to happen at or after nextOffset, otherwise it's probably unrelated to the next token
            if (e.offset >= nextOffset && e.parseException.expectedTokenSequences != null) {
                var expected = e.parseException.expectedTokenSequences;
                return stream(expected).filter(s -> s.length > 0).map(s -> s[0]).collect(toSet());
            }
        } catch (Exception e) {
            // Ignore
        }
        return Set.of();
    }

    private Stream<String> collectKeywords(Stream<Integer> tokenKinds) {
        return tokenKinds
                .filter(kind -> kind != ESCAPED_SYMBOLIC_NAME)
                .map(kind -> CypherConstants.tokenImage[kind])
                .filter(image -> image.length() > 2)
                .map(image -> image.substring(1, image.length() - 1));
    }

    private boolean isIdentifierToken(int tokenKind) {
        return getIdentifierTokens().contains(tokenKind);
    }

    private static class TokenIterator implements Iterator<Token> {
        @SuppressWarnings("rawtypes")
        private final Cypher cypher;

        private org.neo4j.cypher.internal.parser.javacc.Token current;
        private org.neo4j.cypher.internal.parser.javacc.Token next;

        @SuppressWarnings({"rawtypes", "unchecked"})
        private TokenIterator(String query) {
            cypher = new Cypher(new LiteralInterpreter(), new SimpleAstExceptionFactory(), new CypherCharStream(query));
            next = cypher.getNextToken();
        }

        @Override
        public boolean hasNext() {
            return next.kind != CypherConstants.EOF;
        }

        @Override
        public Token next() {
            var previous = current;
            current = next;
            next = cypher.getNextToken();

            if (current.kind == IDENTIFIER && previous != null && previous.kind == CypherConstants.DOLLAR) {
                return new ParameterIdentifierToken(current.image, current.beginOffset, current.endOffset);
            }

            return new SimpleToken(current.kind, current.image, current.beginOffset, current.endOffset);
        }
    }

    private static class ParseSyntaxException extends RuntimeException {
        final int offset;
        final String got;
        final ParseException parseException;

        private ParseSyntaxException(int offset, String got, ParseException parseException) {
            super("", parseException, false, false);
            this.offset = offset;
            this.got = got;
            this.parseException = parseException;
        }
    }

    private static class SimpleAstExceptionFactory implements ASTExceptionFactory {
        @Override
        public Exception syntaxException(
                String got, List<String> expected, Exception source, int offset, int line, int column) {
            if (source instanceof ParseException parseException) {
                return new ParseSyntaxException(offset, got, parseException);
            }
            return source;
        }

        @Override
        public Exception syntaxException(Exception source, int offset, int line, int column) {
            return source;
        }
    }

    record SimpleToken(int kind, String image, int beginOffset, int endOffset) implements Token {
        @Override
        public boolean isParameterIdentifier() {
            return false;
        }
    }

    record ParameterIdentifierToken(String image, int beginOffset, int endOffset) implements Token {
        @Override
        public int kind() {
            return IDENTIFIER;
        }

        @Override
        public boolean isParameterIdentifier() {
            return true;
        }
    }
}
