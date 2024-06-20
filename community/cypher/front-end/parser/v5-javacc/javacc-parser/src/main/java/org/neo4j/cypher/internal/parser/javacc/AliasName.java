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
package org.neo4j.cypher.internal.parser.javacc;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;
// spotless:off
import org.neo4j.cypher.internal.parser.javacc.Token;
import org.neo4j.cypher.internal.parser.javacc.ParseException;
import org.neo4j.cypher.internal.parser.javacc.CypherConstants;
// spotless:on

public class AliasName<DATABASE_NAME, PARAMETER> {
    ASTExceptionFactory exceptionFactory;
    List<Token> names = new ArrayList<>();
    PARAMETER parameter;
    private int componentCount = 1;
    private Token lastToken;

    public AliasName(ASTExceptionFactory exceptionFactory, Token token) throws Exception {
        this.exceptionFactory = exceptionFactory;
        this.lastToken = token;
        this.names.add(token);
    }

    public AliasName(ASTExceptionFactory exceptionFactory, PARAMETER parameter) {
        this.exceptionFactory = exceptionFactory;
        this.parameter = parameter;
    }

    public void add(Token token) throws Exception {
        if (isEscaped(token) || isEscaped(lastToken)) {
            componentCount++;
        }
        lastToken = token;
        names.add(token);
    }

    public DATABASE_NAME getRemoteAliasName(ASTFactory astFactory) throws Exception {
        if (parameter != null) {
            return (DATABASE_NAME) astFactory.databaseName(parameter);
        } else {
            if (names.size() > 2) {
                throw exceptionFactory.syntaxException(
                        new ParseException(ASTExceptionFactory.invalidDotsInRemoteAliasName(originalRepresentation())),
                        names.get(0).beginOffset,
                        names.get(0).beginLine,
                        names.get(0).beginColumn);
            } else {
                return (DATABASE_NAME) astFactory.databaseName(
                        astFactory.inputPosition(
                                names.get(0).beginOffset, names.get(0).beginLine, names.get(0).beginColumn),
                        tokensAsStrings());
            }
        }
    }

    public DATABASE_NAME getAsDatabaseName(ASTFactory astFactory) throws Exception {
        if (parameter != null) {
            return (DATABASE_NAME) astFactory.databaseName(parameter);
        } else {
            if (componentCount > 1) {
                var invalidToken = names.stream().filter(this::isEscaped).findFirst();
                if (invalidToken.isPresent()) {
                    throw exceptionFactory.syntaxException(
                            new ParseException(
                                    ASTExceptionFactory.tooManyDatabaseNameComponents(originalRepresentation())),
                            names.get(0).beginOffset,
                            names.get(0).beginLine,
                            names.get(0).beginColumn);
                }
            }
            return (DATABASE_NAME) astFactory.databaseName(
                    astFactory.inputPosition(
                            names.get(0).beginOffset, names.get(0).beginLine, names.get(0).beginColumn),
                    tokensAsStrings());
        }
    }

    public DATABASE_NAME getLocalAliasName(ASTFactory astFactory) throws Exception {
        if (parameter != null) {
            return (DATABASE_NAME) astFactory.databaseName(parameter);
        } else {
            if (componentCount > 2) {
                var invalidToken = names.stream().filter(this::isEscaped).findFirst();
                if (invalidToken.isPresent()) {
                    throw exceptionFactory.syntaxException(
                            new ParseException(
                                    ASTExceptionFactory.tooManyAliasNameComponents(originalRepresentation())),
                            names.get(0).beginOffset,
                            names.get(0).beginLine,
                            names.get(0).beginColumn);
                }
            }
            return (DATABASE_NAME) astFactory.databaseName(
                    astFactory.inputPosition(
                            names.get(0).beginOffset, names.get(0).beginLine, names.get(0).beginColumn),
                    tokensAsStrings());
        }
    }

    private boolean isEscaped(Token token) {
        return token.kind == CypherConstants.ESCAPED_SYMBOLIC_NAME;
    }

    private List<String> tokensAsStrings() {
        return names.stream().map(t -> t.image).toList();
    }

    private String originalRepresentation() {
        return names.stream()
                .map(token -> {
                    if (isEscaped(token)) {
                        return "`" + token.image + "`";
                    } else {
                        return token.image;
                    }
                })
                .collect(Collectors.joining("."));
    }
}
