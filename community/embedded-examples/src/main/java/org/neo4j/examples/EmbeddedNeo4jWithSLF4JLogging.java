/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.slf4j.Slf4jLogProvider;

public class EmbeddedNeo4jWithSLF4JLogging
{
    private static final String DB_PATH = "target/neo4j-store";
    private static GraphDatabaseService graphDb;

    public static void main( final String[] args ) throws IOException
    {
        FileUtils.deleteRecursively( new File( DB_PATH ) );

        // START SNIPPET: startDbWithSlf4jLogProvider
        graphDb = new GraphDatabaseFactory().setUserLogProvider( new Slf4jLogProvider() ).newEmbeddedDatabase( DB_PATH );
        // END SNIPPET: startDbWithSlf4jLogProvider

        shutdown();
    }

    private static void shutdown()
    {
        graphDb.shutdown();
    }
}
