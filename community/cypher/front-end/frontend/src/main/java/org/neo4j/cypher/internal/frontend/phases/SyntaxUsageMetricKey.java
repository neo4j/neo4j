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
package org.neo4j.cypher.internal.frontend.phases;

public enum SyntaxUsageMetricKey {
    GPM_SHORTEST("GPM_SHORTEST"),
    LEGACY_SHORTEST("LEGACY_SHORTEST"),
    COLLECT_SUBQUERY("COLLECT_SUBQUERY"),
    COUNT_SUBQUERY("COUNT_SUBQUERY"),
    EXISTS_SUBQUERY("EXISTS_SUBQUERY"),
    QUANTIFIED_PATH_PATTERN("QUANTIFIED_PATH_PATTERN");

    public final String key;

    SyntaxUsageMetricKey(String key) {
        this.key = key;
    }
}
