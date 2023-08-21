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
package org.neo4j.kernel.impl.newapi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.Kernel;

/**
 * This interface defines the functionality that's needed to run Kernel API Read tests (tests that extends
 * KernelAPIReadTestBase) on a Kernel.
 */
public interface KernelAPIReadTestSupport {
    /**
     * Setup the test. Called once. Starts a Kernel in the provided store directory, and populates the graph using
     * the provided create method.
     *
     * @param storeDir The directory in which to create the database.
     * @param create Method which populates the database.
     * @param systemCreate Method which populates the system database.
     * @throws IOException If database creation failed due to IO problems.
     */
    void setup(Path storeDir, Consumer<GraphDatabaseService> create, Consumer<GraphDatabaseService> systemCreate);

    /**
     * The Kernel to test. Called before every test.
     * @return The Kernel.
     */
    Kernel kernelToTest();

    /**
     * Teardown the Kernel and any other resources once all tests have completed.
     */
    void tearDown();
}
