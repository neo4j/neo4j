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
package org.neo4j.genai.vector.providers;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf(value = "authIsSet", disabledReason = "token needs to be set in the config map")
public class VertexAIIT {
    private static final String VERTEX_AI_TOKEN_ENV = "VERTEX_AI_TOKEN";
    private static final String VERTEX_AI_PROJECT_ID_ENV = "VERTEX_AI_PROJECT_ID";
    private static final Map<String, ?> BASE_CONFIG;

    static {
        HashMap<String, String> config = new HashMap<>();
        String token = System.getenv(VERTEX_AI_TOKEN_ENV);
        if (token != null) {
            config.put("token", token);
        }
        String projectId = System.getenv(VERTEX_AI_PROJECT_ID_ENV);
        if (token != null) {
            config.put("projectId", projectId);
        }

        BASE_CONFIG = config;
    }

    private static boolean authIsSet() {
        return BASE_CONFIG.containsKey("token");
    }

    @Nested
    class Gecko001 extends BaseIT {
        Gecko001() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko@001.txt",
                    BASE_CONFIG,
                    Map.of("model", "textembedding-gecko@001"));
        }
    }

    @Nested
    class Gecko002 extends BaseIT {
        Gecko002() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko@002.txt",
                    BASE_CONFIG,
                    Map.of("model", "textembedding-gecko@002"));
        }
    }

    @Nested
    class Gecko003 extends BaseIT {
        Gecko003() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko@003.txt",
                    BASE_CONFIG,
                    Map.of("model", "textembedding-gecko@003"));
        }
    }

    @Nested
    class GeckoMultilingual001 extends BaseIT {
        GeckoMultilingual001() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko-multilingual@001.txt",
                    BASE_CONFIG,
                    Map.of("model", "textembedding-gecko-multilingual@001"));
        }
    }

    @Nested
    class Gecko003TaskType extends BaseIT {
        Gecko003TaskType() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko@003-classification.txt",
                    BASE_CONFIG,
                    Map.of("model", "textembedding-gecko@003", "taskType", "CLASSIFICATION"));
        }
    }

    @Nested
    class Gecko003Title extends BaseIT {
        Gecko003Title() {
            super(
                    VertexAI.NAME,
                    "vertexai/textembedding-gecko@003-title.txt",
                    BASE_CONFIG,
                    Map.of(
                            "model",
                            "textembedding-gecko@003",
                            "taskType",
                            "RETRIEVAL_DOCUMENT",
                            "title",
                            "A Document About Titles"));
        }
    }
}
