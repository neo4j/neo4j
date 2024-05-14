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
package org.neo4j.cypher.internal.ast.factory.neo4j;

import java.io.IOException;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.neo4j.cypher.internal.parser.CypherLexer;
import org.neo4j.cypher.internal.parser.lexer.CypherQueryAccess;
import org.neo4j.cypher.internal.parser.lexer.CypherQueryAccess.OffsetTable;
import org.neo4j.cypher.internal.parser.lexer.CypherToken;
import org.neo4j.cypher.internal.parser.lexer.UnicodeEscapeReplacementReader;
import org.neo4j.util.VisibleForTesting;

public final class CypherAstLexer extends CypherLexer implements CypherQueryAccess {
    private final String inputQuery; // Note, not always identical to what the parser sees.
    private final OffsetTable offsetTable;

    private CypherAstLexer(CharStream input, String inputQuery, OffsetTable offsetTable) {
        super(input);
        this.inputQuery = inputQuery;
        this.offsetTable = offsetTable;
    }

    @Override
    public void notifyListeners(LexerNoViableAltException e) {
        String text = _input.getText(Interval.of(_tokenStartCharIndex, _input.index()));
        String msg = "Unexpected query part: '" + getErrorDisplay(text) + "'";

        ANTLRErrorListener listener = getErrorListenerDispatch();
        Token dummyToken = getTokenFactory()
                .create(
                        _tokenFactorySourcePair,
                        EOF,
                        text,
                        _channel,
                        _tokenStartCharIndex,
                        _input.index(),
                        _tokenStartLine,
                        _tokenStartCharPositionInLine);
        listener.syntaxError(this, dummyToken, _tokenStartLine, _tokenStartCharPositionInLine, msg, e);
    }

    /**
     *  Pre-parser step that replace unicode escape sequences.
     *  The returned lexer produces {@link CypherToken}s with input positions
     *  relative to the chars of the input string (which is not always the same
     *  as the codepoint based offset that antlr provides).
     *
     * @param cypher cypher query string
     * @param fullTokens if false the lexer produces optimised ThinCypherTokens, if true full tokens are produced,
     *                   that are needed for some error handling.
     */
    public static CypherAstLexer fromString(final String cypher, boolean fullTokens) throws IOException {
        return fromString(cypher, UnicodeEscapeReplacementReader.DEFAULT_BUFFER_SIZE, fullTokens);
    }

    @VisibleForTesting
    public static CypherAstLexer fromString(final String cypher, int bufferSize, boolean fullTokens)
            throws IOException {
        final var read = UnicodeEscapeReplacementReader.read(cypher, bufferSize);
        final var lexer = new CypherAstLexer(read.charStream(), cypher, read.offsetTable());
        lexer.setTokenFactory(CypherToken.factory(fullTokens));
        return lexer;
    }

    @Override
    public String inputQuery() {
        return inputQuery;
    }

    @Override
    public OffsetTable offsetTable() {
        return offsetTable;
    }
}
