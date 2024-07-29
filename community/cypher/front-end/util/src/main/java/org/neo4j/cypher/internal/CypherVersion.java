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
package org.neo4j.cypher.internal;

public enum CypherVersion {
    Cypher5("5", false),
    Cypher6("6", true);

    public static final CypherVersion Default = Cypher5;

    public final String versionName;
    public final boolean experimental;

    CypherVersion(String versionName, boolean experimantal) {
        this.versionName = versionName;
        this.experimental = experimantal;
    }

    @Override
    public String toString() {
        return versionName;
    }
}
