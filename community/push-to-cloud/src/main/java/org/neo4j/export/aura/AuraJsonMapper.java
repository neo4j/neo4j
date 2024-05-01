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
package org.neo4j.export.aura;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.Arrays;
import java.util.List;

// Simple structs for mapping JSON to objects, used by the jackson parser which Neo4j happens to depend on anyway
public class AuraJsonMapper {

    private AuraJsonMapper() {
        throw new RuntimeException("Class should not be instantiated");
    }

    // Some of these are null in GCP but will not be for AWS once we have that part.
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SignedURIBodyResponse {
        public String[] SignedLinks;
        public String SignedURI;
        public String UploadID;
        public int TotalParts;
        public String Provider;

        @Override
        public String toString() {
            return String.format(
                    "SignedLinks %s, SignedURI %s, UploadID %s, TotalParts %s, Provider %s",
                    Arrays.toString(SignedLinks), SignedURI, UploadID, TotalParts, Provider);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PartEtag {

        public int PartNumber;
        public String ETag;

        @Override
        public String toString() {
            return String.format("[%d:%s]", PartNumber, ETag);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UploadStatusResponse {

        public List<PartEtag> Parts;
        public String UploadID;
        public String Provider;

        @Override
        public String toString() {
            return String.format("UploadID %s, Provider %s Parts: %s", UploadID, Provider, Parts.toString());
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TriggerImportRequest {
        @JsonUnwrapped
        public UploadStatusResponse uploadStatusResponse;

        @JsonProperty("Crc32")
        public long Crc32;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StatusBody {
        public String Status;
        public ErrorBody Error = new ErrorBody();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorBody {
        private static final String DEFAULT_MESSAGE =
                "an unexpected problem occurred, please contact customer support at: https://support.neo4j.com for assistance";
        private static final String DEFAULT_REASON = "UnknownError";

        private final String message;
        private final String reason;
        private final String url;

        public ErrorBody() {
            this(null, null, null);
        }

        @JsonCreator
        public ErrorBody(
                @JsonProperty("Message") String message,
                @JsonProperty("Reason") String reason,
                @JsonProperty("Url") String url) {
            this.message = message;
            this.reason = reason;
            this.url = url;
        }

        public String getMessage() {
            return defaultIfBlank(this.message, DEFAULT_MESSAGE);
        }

        public String getReason() {
            return defaultIfBlank(this.reason, DEFAULT_REASON);
        }

        public String getUrl() {
            return this.url;
        }
    }
}
