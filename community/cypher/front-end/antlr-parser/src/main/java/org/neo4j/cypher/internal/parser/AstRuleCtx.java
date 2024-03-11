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
package org.neo4j.cypher.internal.parser;

import java.util.ArrayList;
import org.antlr.v4.runtime.ParserRuleContext;

public class AstRuleCtx extends ParserRuleContext {
    public Object ast;

    public AstRuleCtx() {}

    public AstRuleCtx(ParserRuleContext parent, int invokingStateNumber) {
        super(parent, invokingStateNumber);

        /*
         * The default size of array lists are 10.
         * But it's very common that we have fewer children.
         * When parsing all the macro benchmark queries,
         * 0.5% of contexts had 0 children,
         * 90% had a single child or less,
         * 95% had two or less,
         * 97% had three or less.
         */
        this.children = new ArrayList<>(3);
    }

    @SuppressWarnings("unchecked")
    public final <T> T ast() {
        return (T) ast;
    }
}
