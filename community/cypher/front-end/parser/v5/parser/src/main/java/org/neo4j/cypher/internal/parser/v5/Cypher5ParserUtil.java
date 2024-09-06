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
package org.neo4j.cypher.internal.parser.v5;

import org.antlr.v4.runtime.dfa.DFA;

public class Cypher5ParserUtil {
    // This method does the same as interpreter.clearDFACache(), without access to the interpreter
    public static synchronized void clearDFACache() {
        for (int i = 0; i < Cypher5Parser._decisionToDFA.length; i++) {
            Cypher5Parser._decisionToDFA[i] = new DFA(Cypher5Parser._ATN.getDecisionState(i), i);
        }
    }
}
