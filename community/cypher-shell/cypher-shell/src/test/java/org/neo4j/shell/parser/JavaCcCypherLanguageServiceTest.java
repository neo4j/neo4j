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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.COUNT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DOT;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.EQ;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LCURLY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LPAREN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MATCH;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RCURLY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RETURN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.RPAREN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING_LITERAL1;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_DECIMAL_INTEGER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.WHERE;

import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.parser.JavaCcCypherLanguageService.SimpleToken;

class JavaCcCypherLanguageServiceTest {
    private final JavaCcCypherLanguageService parser = new JavaCcCypherLanguageService();

    @Test
    void tokenizeEmptyString() {
        assertTokens("");
        assertTokens("     ");
    }

    @Test
    void tokenizeCase() {
        assertTokens("match", token(MATCH, "match", 0, 4));
        assertTokens("MATCH", token(MATCH, "MATCH", 0, 4));
    }

    @Test
    void tokenizeSanity() {
        // Some sanity checking of the tokenizer

        var tokens = List.of(
                token(MATCH, "MATCH", 0, 4),
                token(LPAREN, "(", 6, 6),
                token(IDENTIFIER, "myNode", 7, 12),
                token(":", 13, 13),
                token(IDENTIFIER, "MyLabel", 14, 20),
                token(LCURLY, "{", 22, 22),
                token(IDENTIFIER, "mapKey", 24, 29),
                token(":", 30, 30),
                token(STRING_LITERAL1, "value", 32, 38),
                token(RCURLY, "}", 40, 40),
                token(RPAREN, ")", 41, 41),
                token(WHERE, "WHERE", 43, 47),
                token(IDENTIFIER, "myNode", 49, 54),
                token(DOT, ".", 55, 55),
                token(IDENTIFIER, "p", 56, 56),
                token(EQ, "=", 58, 58),
                token(UNSIGNED_DECIMAL_INTEGER, "1", 60, 60),
                token(RETURN, "RETURN", 62, 67),
                token(COUNT, "count", 69, 73),
                token(LPAREN, "(", 74, 74),
                token(IDENTIFIER, "myNode", 75, 80),
                token(RPAREN, ")", 81, 81));

        assertTokens("MATCH (myNode:MyLabel { mapKey: 'value' }) WHERE myNode.p = 1 RETURN count(myNode)", tokens);

        var queryParts = List.of(
                "MATCH",
                " (",
                "myNode",
                ":",
                "MyLabel",
                " {",
                " mapKey",
                ":",
                " 'value'",
                " }",
                ")",
                " WHERE",
                " myNode",
                ".",
                "p",
                " =",
                " 1",
                " RETURN",
                " count",
                "(",
                "myNode",
                ")");
        for (int i = 1; i <= queryParts.size(); ++i) {
            var query = queryParts.stream().limit(i).collect(Collectors.joining());
            assertTokens(query, tokens.stream().limit(i).toList());
        }
    }

    @Test
    void querySuggestionsSanity() {
        // Some sanity checking of next keyword suggestions

        assertSuggestions(
                "", "CREATE", "MATCH", "DROP", "UNWIND", "RETURN", "WITH", "LOAD", "ALTER", "RENAME", "SHOW", "START",
                "STOP");
        assertSuggestions("MATCH (n) ", "WHERE", "MATCH", "RETURN", "UNWIND", "MERGE", "DELETE", "WITH", "UNION");
        assertSuggestions("MATCH (n) WHERE", "CASE", "ANY", "ALL");
        assertSuggestions("MATCH (n) WHERE n.p", "IN", "ENDS", "STARTS", "CONTAINS", "IS", "RETURN");
        assertSuggestions("MATCH (n) WHERE n.p = 1", "AND", "OR", "RETURN");
    }

    @Test
    void keywordsSanity() {
        // Some sanity checking of keywords

        var keywords = parser.keywords().toList();

        assertThat(keywords)
                .contains("MATCH", "OPTIONAL", "UNWIND", "WHERE", "CONTAINS", "IN", "CREATE", "MERGE", "SET", "DELETE");

        assertThat(keywords.stream().filter(String::isBlank)).isEmpty();
    }

    private Condition<CypherLanguageService.Token> token(int kind, String image, int begin, int end) {
        SimpleToken token = new SimpleToken(kind, image, begin, end);
        return new Condition<>(
                token::equals, "Token with image=" + image + ", beginOffset=" + begin + ", endOffset=" + end);
    }

    private Condition<CypherLanguageService.Token> token(String image, int begin, int end) {
        return new Condition<>(
                token -> token.image().equals(image) && token.beginOffset() == begin && token.endOffset() == end,
                "Token with image=" + image + ", beginOffset=" + begin + ", endOffset=" + end);
    }

    @SafeVarargs
    private void assertTokens(String query, Condition<CypherLanguageService.Token>... expected) {
        assertTokens(query, List.of(expected));
    }

    private void assertTokens(String query, List<Condition<CypherLanguageService.Token>> expected) {
        assertThat(parser.tokenize(query))
                .as("Parser tokenizes query")
                .zipSatisfy(expected, (token, condition) -> assertThat(token).is(condition));
    }

    private void assertSuggestions(String query, String... expected) {
        assertThat(parser.suggestNextKeyword(query.toLowerCase())).contains(expected);
        assertThat(parser.suggestNextKeyword(query)).contains(expected);
    }
}
