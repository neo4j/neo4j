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

@EnabledIf(value = "authIsSet", disabledReason = "accessKeyId and secretAccessKey needs to be set in the config map")
public class BedrockIT {
    private static final String AWS_ACCESS_KEY_ID_ENV = "AWS_ACCESS_KEY";
    private static final String AWS_SECRET_ACCESS_KEY_ENV = "AWS_SECRET";
    private static final Map<String, ?> BASE_CONFIG;

    static {
        HashMap<String, String> config = new HashMap<>();
        Map<String, String> env = System.getenv();
        String accessKey = env.get(AWS_ACCESS_KEY_ID_ENV);
        if (accessKey != null) {
            config.put("accessKeyId", accessKey);
        }
        String secret = env.get(AWS_SECRET_ACCESS_KEY_ENV);
        if (secret != null) {
            config.put("secretAccessKey", secret);
        }

        BASE_CONFIG = config;
    }

    @Nested
    class TitanEmbedTextG1 extends BaseIT {
        TitanEmbedTextG1() {
            super(
                    Bedrock.NAME,
                    "bedrock/amazon.titan-embed-text-v1.txt",
                    BASE_CONFIG,
                    Map.of("model", "amazon.titan-embed-text-v1"));
        }
    }

    @Nested
    class TitanEmbedImageG1 extends BaseIT {
        TitanEmbedImageG1() {
            super(
                    Bedrock.NAME,
                    "bedrock/amazon.titan-embed-image-v1.txt",
                    BASE_CONFIG,
                    Map.of("model", "amazon.titan-embed-image-v1"));
        }
    }

    @Nested
    class TitanEmbedTextV2 extends BaseIT {
        TitanEmbedTextV2() {
            super(
                    Bedrock.NAME,
                    "bedrock/amazon.titan-embed-text-v2_0.txt",
                    BASE_CONFIG,
                    Map.of("model", "amazon.titan-embed-text-v2:0"));
        }
    }

    private static boolean authIsSet() {
        return BASE_CONFIG.containsKey("accessKeyId") && BASE_CONFIG.containsKey("secretAccessKey");
    }
}
