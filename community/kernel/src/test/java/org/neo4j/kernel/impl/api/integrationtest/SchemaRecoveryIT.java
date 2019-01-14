/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.File;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.subprocess.SubProcess;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class SchemaRecoveryIT
{
    @Test
    public void schemaTransactionsShouldSurviveRecovery() throws Exception
    {
        // given
        File storeDir = testDirectory.absolutePath();
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
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private List<ConstraintDefinition> constraints( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return Iterables.asList( database.schema().getConstraints() );
        }
    }

    private List<IndexDefinition> indexes( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return Iterables.asList( database.schema().getIndexes() );
        }
    }

    public interface Process
    {
        void waitForSchemaTransactionCommitted() throws InterruptedException;
    }

    static class CreateConstraintButDoNotShutDown extends SubProcess<Process, File> implements Process
    {
        // Would use a CountDownLatch but fields of this class need to be serializable.
        private volatile boolean started;

        @Override
        protected void startup( File storeDir )
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
            while ( !started )
            {
                Thread.sleep( 10 );
            }
        }
    }
}
