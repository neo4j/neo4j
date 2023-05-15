/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

package org.neo4j.export;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Simple structs for mapping JSON to objects, used by the jackson parser which Neo4j happens to depend on anyway
class AuraResponse {

    private AuraResponse() {
        throw new RuntimeException("Class should not be instantiated");
    }

    // Some of these are null in GCP but will not be for AWS once we have that part.
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SignedURIBody {
        public String[] SignedLinks;
        public String SignedURI;
        public String UploadID;
        public int TotalParts;
        public String Provider;
    }
}
