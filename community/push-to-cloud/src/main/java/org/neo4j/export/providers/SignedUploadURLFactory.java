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
package org.neo4j.export.providers;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.CommandResponseHandler;
import org.neo4j.export.UploadURLFactory;
import org.neo4j.export.aura.AuraJsonMapper.SignedURIBodyResponse;

public class SignedUploadURLFactory implements UploadURLFactory {

    public enum Provider {
        AWS("AWS"),
        GCP("GCP");
        private final String name;

        Provider(String name) {
            this.name = name;
        }
    }

    public SignedUploadURLFactory() {}

    // This will eventually become more complicated as we add more providers
    @Override
    public SignedUpload fromAuraResponse(
            SignedURIBodyResponse signedURIBodyResponse, ExecutionContext ctx, String boltURI) {
        if (signedURIBodyResponse.Provider.equalsIgnoreCase(SignedUploadURLFactory.Provider.AWS.name)) {
            return new org.neo4j.export.providers.SignedUploadAWS(
                    signedURIBodyResponse.SignedLinks,
                    signedURIBodyResponse.UploadID,
                    signedURIBodyResponse.TotalParts,
                    ctx,
                    boltURI);
        }
        return new SignedUploadGCP(
                signedURIBodyResponse.SignedLinks,
                signedURIBodyResponse.SignedURI,
                ctx,
                boltURI,
                new CommandResponseHandler(ctx));
    }

    static class RetryableHttpException extends RuntimeException {
        RetryableHttpException(CommandFailedException e) {
            super(e);
        }
    }
}
