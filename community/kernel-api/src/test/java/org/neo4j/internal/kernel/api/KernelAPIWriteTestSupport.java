/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;


/**
 * This interface defines the functionality that's needed to run Kernel API Write tests (tests that extends
 * KernelAPIWriteTestBase) on a Kernel.
 */
public interface KernelAPIWriteTestSupport
{
    /**
     * Create the Kernel to test in the provided directory.
     * @param storeDir The directory to hold the database
     * @throws IOException Thrown on IO failure during database creation
     */
    void setup( File storeDir ) throws IOException;

    /**
     * Clear the graph. Executed before each test.
     */
    void clearGraph();

    /**
     * Return the Kernel to test. Executed before each test.
     */
    Kernel kernelToTest();

    /**
     * Backdoor to allow asserting on write effects
     */
    GraphDatabaseService graphBackdoor();

    /**
     * Clean up resources and close the database. Executed after all tests are completed.
     */
    void tearDown();
}
