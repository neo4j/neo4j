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
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintException;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.api.impl.schema.NativeLuceneFusionSchemaIndexProviderFactory.subProviderDirectoryStructure;

public class ConstraintIndexFailureIT
{
    private static final String INJECTED_FAILURE = "Injected failure";

    public final TestDirectory storeDir = TestDirectory.testDirectory();
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain
            .outerRule( storeDir )
            .around( fileSystemRule )
            .around( pageCacheRule );

    @Test
    public void shouldFailToValidateConstraintsIfUnderlyingIndexIsFailed() throws Exception
    {
        // given
        dbWithConstraint();
        storeIndexFailure( INJECTED_FAILURE );

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
                assertThat( e.getCause(), instanceOf( UnableToValidateConstraintException.class ) );
                assertThat( e.getCause().getCause().getMessage(), allOf(
                        containsString( "The index is in a failed state:" ),
                        containsString( INJECTED_FAILURE ) ) );
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
        File luceneIndexDirectory = subProviderDirectoryStructure( storeDir.directory() )
                .forProvider( PROVIDER_DESCRIPTOR ).directoryForIndex( 1 );
        DirectoryFactory dirFactory =
                DirectoryFactory.newDirectoryFactory( pageCacheRule.getPageCache( fileSystemRule ), Config.defaults(),
                        NullLog.getInstance() );
        PartitionedIndexStorage indexStorage = LuceneIndexStorageBuilder.create( dirFactory )
                .withFileSystem( fileSystemRule.get() )
                .withIndexFolder( luceneIndexDirectory )
                .build();
        indexStorage.storeIndexFailure( failure );
    }
}
