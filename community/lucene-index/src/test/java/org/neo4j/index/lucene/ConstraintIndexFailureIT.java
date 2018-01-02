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
package org.neo4j.index.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintKernelException;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.kernel.api.index.util.FolderLayout;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.TargetDirectory.testDirForTest;

public class ConstraintIndexFailureIT
{
    public final @Rule TargetDirectory.TestDirectory storeDir = testDirForTest( ConstraintIndexFailureIT.class );

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
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir.directory().getAbsolutePath() );
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
        new FailureStorage( new DefaultFileSystemAbstraction(), new FolderLayout( luceneRootDirectory ) )
                .storeIndexFailure( singleIndexId( luceneRootDirectory ), failure );
    }

    private int singleIndexId( File luceneRootDirectory )
    {
        List<Integer> indexIds = new ArrayList<>();
        for ( String file : luceneRootDirectory.list() )
        {
            try
            {
                indexIds.add( Integer.parseInt( file ) );
            }
            catch ( NumberFormatException e )
            {
                // do nothing
            }
        }
        return IteratorUtil.single( indexIds );
    }
}
