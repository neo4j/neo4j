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
package org.neo4j.export.util;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.cli.CommandTestUtils.withSuppressedOutput;
import static wiremock.org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import wiremock.org.hamcrest.Matcher;

public class ExportTestUtilities {

    public static void createDump(Path homeDir, Path confDir, Path dump, FileSystemAbstraction fs, String dbName) {
        withSuppressedOutput(homeDir, confDir, fs, ctx -> {
            final var dumpCommand = new DumpCommand(ctx);
            picocli.CommandLine.populateCommand(dumpCommand, "--to-path=" + dump, dbName);
            assertThatCode(dumpCommand::execute).doesNotThrowAnyException();
        });
    }

    public static void prepareDatabase(DatabaseLayout databaseLayout) {
        new TestDatabaseManagementServiceBuilder(databaseLayout).build().shutdown();
    }

    public static void assertThrows(
            Class<? extends Exception> exceptionClass, Matcher<String> message, ThrowingRunnable action) {
        try {
            action.run();
            fail("Should have failed");
        } catch (Exception e) {
            assertTrue(exceptionClass.isInstance(e));
            assertThat(e.getMessage(), message);
        }
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static class ControlledProgressListener extends ProgressListener.Adapter {
        public long progress;
        public boolean closeCalled;

        @Override
        public void add(long progress) {
            this.progress += progress;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public void failed(Throwable e) {
            throw new UnsupportedOperationException("Should not be called");
        }
    }
}
