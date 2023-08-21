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

import static java.io.OutputStream.nullOutputStream;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.aura.AuraJsonMapper;
import org.neo4j.export.providers.SignedUpload;
import org.neo4j.export.providers.SignedUploadAWS;
import org.neo4j.export.providers.SignedUploadGCP;
import org.neo4j.export.providers.SignedUploadURLFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class SignedUploadURLFactoryTest {
    @Inject
    TestDirectory directory;

    @Test
    public void fromAuraResponseTest() {
        AuraJsonMapper.SignedURIBodyResponse signedURIbodyResponse = new AuraJsonMapper.SignedURIBodyResponse();
        signedURIbodyResponse.Provider = "AWS";
        Path dir = directory.homePath();
        PrintStream out = new PrintStream(nullOutputStream());
        ExecutionContext ctx = new ExecutionContext(dir, dir, out, out, new DefaultFileSystemAbstraction());

        SignedUpload expectedAWSSignedUpload =
                new SignedUploadURLFactory().fromAuraResponse(signedURIbodyResponse, ctx, "sausage");
        assertThat(expectedAWSSignedUpload).isInstanceOf(SignedUploadAWS.class);
    }

    @Test
    public void fromAuraGCPResponseTest() {
        AuraJsonMapper.SignedURIBodyResponse signedURIbodyResponse = new AuraJsonMapper.SignedURIBodyResponse();
        signedURIbodyResponse.Provider = "GCP";
        Path dir = directory.homePath();
        PrintStream out = new PrintStream(nullOutputStream());
        ExecutionContext ctx = new ExecutionContext(dir, dir, out, out, new DefaultFileSystemAbstraction());

        SignedUpload expectedGCPSignedUpload =
                new SignedUploadURLFactory().fromAuraResponse(signedURIbodyResponse, ctx, "sausage");
        assertThat(expectedGCPSignedUpload).isInstanceOf(SignedUploadGCP.class);
    }
}
