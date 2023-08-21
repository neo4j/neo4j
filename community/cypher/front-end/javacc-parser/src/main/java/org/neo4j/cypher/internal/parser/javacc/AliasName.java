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
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;

public class AliasName<DATABASE_NAME, PARAMETER> {
    ASTExceptionFactory exceptionFactory;
    Token start;
    List<String> names = new ArrayList<>();
    PARAMETER parameter;

    public AliasName(ASTExceptionFactory exceptionFactory, Token token) {
        this.exceptionFactory = exceptionFactory;
        this.start = token;
        this.names.add(token.image);
    }

    public AliasName(ASTExceptionFactory exceptionFactory, PARAMETER parameter) {
        this.exceptionFactory = exceptionFactory;
        this.parameter = parameter;
    }

    public void add(Token token) {
        names.add(token.image);
    }

    public DATABASE_NAME getRemoteAliasName(ASTFactory astFactory) throws Exception {
        if (parameter != null) {
            return (DATABASE_NAME) astFactory.databaseName(parameter);
        } else {
            if (names.size() > 2) {
                throw exceptionFactory.syntaxException(
                        new ParseException(ASTExceptionFactory.invalidDotsInRemoteAliasName(String.join(".", names))),
                        start.beginOffset,
                        start.beginLine,
                        start.beginColumn);
            } else {
                return (DATABASE_NAME) astFactory.databaseName(
                        astFactory.inputPosition(start.beginOffset, start.beginLine, start.beginColumn), names);
            }
        }
    }

    public DATABASE_NAME getLocalAliasName(ASTFactory astFactory) {
        if (parameter != null) {
            return (DATABASE_NAME) astFactory.databaseName(parameter);
        } else {
            return (DATABASE_NAME) astFactory.databaseName(
                    astFactory.inputPosition(start.beginOffset, start.beginLine, start.beginColumn), names);
        }
    }
}
