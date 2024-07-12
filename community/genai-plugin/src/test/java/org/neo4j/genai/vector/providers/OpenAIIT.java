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
class OpenAIIT {
    private static final String OPEN_AI_TOKEN_ENV = "OPEN_AI_TOKEN";
    private static final Map<String, ?> BASE_CONFIG;

    static {
        HashMap<String, Object> config = new HashMap<>();
        String token = System.getenv(OPEN_AI_TOKEN_ENV);
        if (token != null) {
            config.put("token", token);
        }

        BASE_CONFIG = config;
    }

    private static boolean authIsSet() {
        return BASE_CONFIG.containsKey("token");
    }

    @Nested
    class Ada extends BaseIT {
        Ada() {
            super(
                    OpenAI.NAME,
                    "openai/text-embedding-ada-002.txt",
                    BASE_CONFIG,
                    Map.of("model", "text-embedding-ada-002"));
        }
    }

    @Nested
    class LargeWithDefaultDimensions extends BaseIT {
        LargeWithDefaultDimensions() {
            super(
                    OpenAI.NAME,
                    "openai/text-embedding-3-large.txt",
                    BASE_CONFIG,
                    Map.of("model", "text-embedding-3-large"));
        }
    }

    @Nested
    class LargeWith1024Dimensions extends BaseIT {
        LargeWith1024Dimensions() {
            super(
                    OpenAI.NAME,
                    "openai/text-embedding-3-large-1024.txt",
                    BASE_CONFIG,
                    Map.of("model", "text-embedding-3-large", "dimensions", 1024));
        }
    }

    @Nested
    class SmallWithDefaultDimensions extends BaseIT {
        SmallWithDefaultDimensions() {
            super(
                    OpenAI.NAME,
                    "openai/text-embedding-3-small.txt",
                    BASE_CONFIG,
                    Map.of("model", "text-embedding-3-small"));
        }
    }

    @Nested
    class SmallWith512Dimensions extends BaseIT {
        SmallWith512Dimensions() {
            super(
                    OpenAI.NAME,
                    "openai/text-embedding-3-small-512.txt",
                    BASE_CONFIG,
                    Map.of("model", "text-embedding-3-small", "dimensions", 512));
        }
    }
}
