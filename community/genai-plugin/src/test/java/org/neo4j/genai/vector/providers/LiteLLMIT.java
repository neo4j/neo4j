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

@EnabledIf(value = "modelIsSet", disabledReason = "model need to be set in the config map")
class LiteLLMIT {
    private static final String LITE_LLM_ENDPOINT_ENV = "LITE_LLM_ENDPOINT";
    private static final String LITE_LLM_MODEL_ENV = "LITE_LLM_MODEL";
    private static final Map<String, ?> BASE_CONFIG;

    private static final String MODEL;

    static {
        HashMap<String, Object> config = new HashMap<>();
        String endpoint = System.getenv(LITE_LLM_ENDPOINT_ENV);
        if (endpoint != null) {
            config.put("endpoint", endpoint);
        }

        MODEL = System.getenv(LITE_LLM_MODEL_ENV);

        BASE_CONFIG = config;
    }

    private static boolean modelIsSet() {
        return BASE_CONFIG.containsKey("endpoint") && MODEL != null;
    }

    @Nested
    class Default extends BaseIT {
        Default() {
            super(LiteLLM.NAME, "ollama/nomic-embed-text", BASE_CONFIG, Map.of("model", MODEL));
        }
    }
}

