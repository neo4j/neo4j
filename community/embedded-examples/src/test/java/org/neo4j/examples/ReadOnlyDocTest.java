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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * How to get a read-only Neo4j instance.
 */
public class ReadOnlyDocTest
{
    protected GraphDatabaseService graphDb;

    /**
     * Create read only database.
     */
    @Before
    public void prepareReadOnlyDatabase() throws IOException
    {
        File dir = new File( "target/read-only-db/location" );
        if ( dir.exists() )
        {
            FileUtils.deleteRecursively( dir );
        }
        new GraphDatabaseFactory().newEmbeddedDatabase(
                "target/read-only-db/location" )
                                  .shutdown();
        // START SNIPPET: createReadOnlyInstance
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                "target/read-only-db/location" )
                                            .setConfig( GraphDatabaseSettings.read_only, "true" )
                                            .newGraphDatabase();
        // END SNIPPET: createReadOnlyInstance
    }

    /**
     * Shutdown the database.
     */
    @After
    public void shutdownDatabase()
    {
        graphDb.shutdown();
    }

    @Test
    public void makeSureDbIsOnlyReadable()
    {
        // when
        Transaction tx = graphDb.beginTx();
        try
        {
            graphDb.createNode();
            tx.success();
            tx.close();
            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException e )
        {
            // ok
            assertThat( e.getCause(), instanceOf( ReadOnlyDbException.class ) );
        }
    }
}
