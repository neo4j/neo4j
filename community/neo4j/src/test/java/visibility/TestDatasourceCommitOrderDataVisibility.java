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
package visibility;

import java.util.Collection;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestDatasourceCommitOrderDataVisibility
{
    private static final String INDEX_NAME = "foo";
    private static final String INDEX_KEY = "bar";
    private static final String INDEX_VALUE = "baz";
    private static final String PROPERTY_NAME = "quux";
    private static final int PROPERTY_VALUE = 42;

    private GraphDatabaseService graphDatabaseService;

    @Before
    public void setUp() throws Exception
    {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void shouldNotMakeIndexWritesVisibleUntilCommit() throws Exception
    {
        Node commonNode;
        try(Transaction tx = graphDatabaseService.beginTx())
        {
            commonNode = graphDatabaseService.createNode();
            tx.success();
        }

        try(Transaction transaction = graphDatabaseService.beginTx())
        {
            // index write first so that that datastore is added first
            graphDatabaseService.index().forNodes( INDEX_NAME ).add( commonNode, INDEX_KEY, INDEX_VALUE );
            commonNode.setProperty( PROPERTY_NAME, PROPERTY_VALUE );

            assertNodeIsNotIndexedOutsideThisTransaction();
            assertNodeIsUnchangedOutsideThisTransaction(commonNode);

            transaction.success();

            assertNodeIsNotIndexedOutsideThisTransaction();
            assertNodeIsUnchangedOutsideThisTransaction(commonNode);
        }

        assertNodeIsIndexed(commonNode);
        assertNodeHasBeenUpdated( commonNode );
    }

    private void assertNodeIsNotIndexedOutsideThisTransaction() throws Exception
    {
        final Collection<Exception> problems = new HashSet<>();

        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try(Transaction ignored = graphDatabaseService.beginTx())
                {
                    assertThat( graphDatabaseService.index().forNodes( INDEX_NAME ).get( INDEX_KEY,
                            INDEX_VALUE ).size(), is( 0 ) );
                }
                catch ( Throwable t )
                {
                    problems.add( new Exception( t ) );
                }
            }
        } );
        thread.start();
        thread.join();

        if ( problems.size() > 0 )
        {
            throw problems.iterator().next();
        }
    }

    private void assertNodeIsUnchangedOutsideThisTransaction( final Node commonNode ) throws Exception
    {
        final Collection<Exception> problems = new HashSet<>();

        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try(Transaction ignored = graphDatabaseService.beginTx())
                {
                    assertThat( commonNode.hasProperty( PROPERTY_NAME ), is( false ) );
                }
                catch ( Throwable t )
                {
                    problems.add( new Exception( t ) );
                }
            }
        } );
        thread.start();
        thread.join();

        if ( problems.size() > 0 )
        {
            throw problems.iterator().next();
        }
    }

    private void assertNodeIsIndexed(final Node commonNode) throws Exception
    {
        final Collection<Exception> problems = new HashSet<>();

        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try(Transaction ignored = graphDatabaseService.beginTx())
                {
                    Node node = graphDatabaseService.index().forNodes( INDEX_NAME ).get( INDEX_KEY,
                            INDEX_VALUE ).getSingle();
                    assertThat( node, is( commonNode) );
                }
                catch ( Throwable t )
                {
                    problems.add( new Exception( t ) );
                }
            }
        } );
        thread.start();
        thread.join();

        if ( problems.size() > 0 )
        {
            throw problems.iterator().next();
        }
    }

    private void assertNodeHasBeenUpdated( final Node commonNode ) throws Exception
    {
        final Collection<Exception> problems = new HashSet<>();

        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try(Transaction ignored = graphDatabaseService.beginTx())
                {
                    assertThat( (Integer) commonNode.getProperty( PROPERTY_NAME ),
                            is( PROPERTY_VALUE ) );
                }
                catch ( Throwable t )
                {
                    problems.add( new Exception( t ) );
                }
            }
        } );
        thread.start();
        thread.join();

        if ( problems.size() > 0 )
        {
            throw problems.iterator().next();
        }
    }
}
