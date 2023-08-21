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
package org.neo4j.export;

import static org.neo4j.cli.CommandType.UPLOAD;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.CommandType;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.aura.AuraClient;
import org.neo4j.export.aura.AuraURLFactory;
import org.neo4j.export.providers.SignedUploadURLFactory;

@ServiceProvider
public class UploadCommandProvider implements CommandProvider {
    @Override
    public UploadCommand createCommand(ExecutionContext ctx) {
        return new UploadCommand(
                ctx,
                new AuraClient.AuraClientBuilder(ctx),
                new AuraURLFactory(),
                new SignedUploadURLFactory(),
                PushToCloudCLI.realConsole());
    }

    @Override
    public CommandType commandType() {
        return UPLOAD;
    }
}
