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
package org.neo4j.cypher.internal.parser.v6;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class Cypher6ParserUtilTest {

    /*
     * The clearing of the DFA cache relies on ANTLR internals.
     * In case this tests start to fail, it is an indication that the ANTLR implementation has changed.
     * Then, the implementation in Cypher5ParserUtils, Cypher6ParserUtils and this test need to be adapted.
     */
    @Test
    void assertDFACacheClearingDoesNotChange() throws IOException {

        String expectedDFAImplementation =
                """
				    static {
				        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
				        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
				            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
				        }
				    }
				""";

        assertThat(readGeneratedParserSourceFile()).contains(expectedDFAImplementation.replace("    ", "\t"));
    }

    private String readGeneratedParserSourceFile() throws IOException {
        Path generatedParserPath = Path.of("src/main/java/org/neo4j/cypher/internal/parser/v6/Cypher6Parser.java");
        return Files.readString(generatedParserPath);
    }
}
