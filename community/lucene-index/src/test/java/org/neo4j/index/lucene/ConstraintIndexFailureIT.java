/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintKernelException;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

public class ConstraintIndexFailureIT
{
    @Rule
    public final TestDirectory storeDir = TestDirectory.testDirectory();

    @Test
    public void shouldFailToValidateConstraintsIfUnderlyingIndexIsFailed() throws Exception
    {
        // given
        dbWithConstraint();
        storeIndexFailure( "Injected failure" );

        // when
        GraphDatabaseService db = startDatabase();
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );

                fail( "expected exception" );
            }
            // then
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getCause(), instanceOf( UnableToValidateConstraintKernelException.class ) );
                assertThat( e.getCause().getCause().getMessage(), equalTo( "The index is in a failed state: 'Injected failure'.") );
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService startDatabase()
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir.directory().getAbsoluteFile() );
    }

    private void dbWithConstraint()
    {
        GraphDatabaseService db = startDatabase();
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label( "Label1" ) ).assertPropertyIsUnique( "key1" ).create();

                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private void storeIndexFailure( String failure ) throws IOException
    {
        File luceneRootDirectory = new File( storeDir.directory(), "schema/index/lucene" );
        PartitionedIndexStorage indexStorage = LuceneIndexStorageBuilder.create()
                .withIndexRootFolder( luceneRootDirectory )
                .withIndexIdentifier( "1" ).build();
        indexStorage.storeIndexFailure( failure );
    }
}
