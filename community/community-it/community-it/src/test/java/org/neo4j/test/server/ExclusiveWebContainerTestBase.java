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
package org.neo4j.test.server;

import static org.neo4j.test.extension.SuppressOutput.suppressAll;

import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
public class ExclusiveWebContainerTestBase {
    @Inject
    protected TestDirectory testDirectory;

    @Inject
    protected SuppressOutput suppressOutput;

    protected String methodName;

    @BeforeAll
    public static void ensureServerNotRunning() throws Exception {
        System.setProperty("org.neo4j.useInsecureCertificateGeneration", "true");
        suppressAll().call((Callable<Void>) () -> {
            WebContainerHolder.ensureNotRunning();
            return null;
        });
    }

    @BeforeEach
    public void init(TestInfo testInfo) {
        methodName = testInfo.getTestMethod().get().getName();
    }

    protected static String txEndpoint() {
        return txEndpoint("neo4j");
    }

    private static String txEndpoint(String database) {
        return String.format("db/%s/tx", database);
    }

    protected static String txCommitEndpoint() {
        return txCommitEndpoint("neo4j");
    }

    protected static String txCommitEndpoint(String database) {
        return txEndpoint(database) + "/commit";
    }
}
