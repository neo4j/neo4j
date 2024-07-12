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
class AzureOpenAIIT {
    private static final String AZURE_OPEN_AI_TOKEN_ENV = "AZURE_OPEN_AI_TOKEN";
    private static final String AZURE_OPEN_AI_RESOURCE_ENV = "AZURE_OPEN_AI_RESOURCE";

    // The model to use is configured on Azure when setting up a deployment.
    // Thus the environment needs to specify one deployment for each model to use here.
    private static final String AZURE_OPEN_AI_DEPLOYMENT_ADA_ENV = "AZURE_OPEN_AI_DEPLOYMENT_ADA";
    private static final String AZURE_OPEN_AI_DEPLOYMENT_LARGE_ENV = "AZURE_OPEN_AI_DEPLOYMENT_LARGE";
    private static final String AZURE_OPEN_AI_DEPLOYMENT_SMALL_ENV = "AZURE_OPEN_AI_DEPLOYMENT_SMALL";
    private static final Map<String, ?> BASE_CONFIG;

    private static final String DEPLOYMENT_ADA;
    private static final String DEPLOYMENT_LARGE;
    private static final String DEPLOYMENT_SMALL;

    static {
        HashMap<String, Object> config = new HashMap<>();
        setFromEnv(config, "token", AZURE_OPEN_AI_TOKEN_ENV);
        setFromEnv(config, "resource", AZURE_OPEN_AI_RESOURCE_ENV);

        DEPLOYMENT_ADA = System.getenv(AZURE_OPEN_AI_DEPLOYMENT_ADA_ENV);
        DEPLOYMENT_LARGE = System.getenv(AZURE_OPEN_AI_DEPLOYMENT_LARGE_ENV);
        DEPLOYMENT_SMALL = System.getenv(AZURE_OPEN_AI_DEPLOYMENT_SMALL_ENV);

        BASE_CONFIG = config;
    }

    private static void setFromEnv(HashMap<String, Object> config, String key, String envVar) {
        String value = System.getenv(envVar);
        if (value != null) {
            config.put(key, value);
        }
    }

    private static boolean authIsSet() {
        return BASE_CONFIG.containsKey("token") && BASE_CONFIG.containsKey("resource");
    }

    @Nested
    class Ada extends BaseIT {
        Ada() {
            super(
                    AzureOpenAI.NAME,
                    "openai/text-embedding-ada-002.txt",
                    BASE_CONFIG,
                    Map.of("deployment", DEPLOYMENT_ADA));
        }
    }

    @Nested
    class LargeWithDefaultDimensions extends BaseIT {
        LargeWithDefaultDimensions() {
            super(
                    AzureOpenAI.NAME,
                    "openai/text-embedding-3-large.txt",
                    BASE_CONFIG,
                    Map.of("deployment", DEPLOYMENT_LARGE));
        }
    }

    @Nested
    class LargeWith1024Dimensions extends BaseIT {
        LargeWith1024Dimensions() {
            super(
                    AzureOpenAI.NAME,
                    "openai/text-embedding-3-large-1024.txt",
                    BASE_CONFIG,
                    Map.of("deployment", DEPLOYMENT_LARGE, "dimensions", 1024));
        }
    }

    @Nested
    class SmallWithDefaultDimensions extends BaseIT {
        SmallWithDefaultDimensions() {
            super(
                    AzureOpenAI.NAME,
                    "openai/text-embedding-3-small.txt",
                    BASE_CONFIG,
                    Map.of("deployment", DEPLOYMENT_SMALL));
        }
    }

    @Nested
    class SmallWith512Dimensions extends BaseIT {
        SmallWith512Dimensions() {
            super(
                    AzureOpenAI.NAME,
                    "openai/text-embedding-3-small-512.txt",
                    BASE_CONFIG,
                    Map.of("deployment", DEPLOYMENT_SMALL, "dimensions", 512));
        }
    }
}
