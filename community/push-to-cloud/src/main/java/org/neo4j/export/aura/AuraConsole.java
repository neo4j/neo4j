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

import java.net.URL;
import org.neo4j.export.util.IOCommon;

public record AuraConsole(String baseURL, String database) {

    public URL getAuthenticateUrl() {
        return IOCommon.safeUrl(String.format("%s/v1/databases/%s/import/auth", baseURL, database));
    }

    public URL getSizeUrl() {
        return IOCommon.safeUrl(String.format("%s/v1/databases/%s/import/size", baseURL, database));
    }

    public URL getStatusUrl() {
        return IOCommon.safeUrl(String.format("%s/v1/databases/%s/import/status", baseURL, database));
    }

    public URL getUploadStatusUrl() {
        return IOCommon.safeUrl(String.format("%s/v1/databases/%s/import/multipart-upload-status", baseURL, database));
    }

    public URL getUploadCompleteUrl() {
        return IOCommon.safeUrl(String.format("%s/v1/databases/%s/import/upload-complete", baseURL, database));
    }

    public URL getImportUrl() {
        return IOCommon.safeUrl(String.format("%s/v2/databases/%s/import", baseURL, database));
    }
}
