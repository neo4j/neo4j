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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.subprocess.SubProcess;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class SchemaRecoveryIT
{
    @Test
    public void schemaTransactionsShouldSurviveRecovery() throws Exception
    {
        // given
        String storeDir = testDirectory.absolutePath();
        Process process = new CreateConstraintButDoNotShutDown().start( storeDir );
        process.waitForSchemaTransactionCommitted();
        SubProcess.kill( process );

        // when
        GraphDatabaseService recoveredDatabase = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        // then
        assertEquals(1, constraints( recoveredDatabase ).size());
        assertEquals(1, indexes( recoveredDatabase ).size());

        recoveredDatabase.shutdown();
    }

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private List<ConstraintDefinition> constraints( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return asList( database.schema().getConstraints() );
        }
    }

    private List<IndexDefinition> indexes( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return asList( database.schema().getIndexes() );
        }
    }

    public interface Process
    {
        void waitForSchemaTransactionCommitted() throws InterruptedException;
    }

    static class CreateConstraintButDoNotShutDown extends SubProcess<Process, String> implements Process
    {
        // Would use a CountDownLatch but fields of this class need to be serializable.
        private volatile boolean started = false;

        @Override
        protected void startup( String storeDir ) throws Throwable
        {
            GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            try ( Transaction transaction = database.beginTx() )
            {
                database.schema().constraintFor( label("User") ).assertPropertyIsUnique( "uuid" ).create();
                transaction.success();
            }
            started = true;
        }

        @Override
        public void waitForSchemaTransactionCommitted() throws InterruptedException
        {
            while (!started)
            {
                Thread.sleep( 10 );
            }
        }
    }
}
