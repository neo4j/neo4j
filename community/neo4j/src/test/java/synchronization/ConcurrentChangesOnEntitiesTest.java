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
package synchronization;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentChangesOnEntitiesTest
{

    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( suppressOutput ).around( testDirectory );

    private final CyclicBarrier barrier = new CyclicBarrier( 2 );
    private final AtomicReference<Exception> ex = new AtomicReference<>();
    private GraphDatabaseService db;

    @Before
    public void setup()
    {
        db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .newGraphDatabase();
    }

    @Test
    public void addConcurrentlySameLabelToANode() throws Throwable
    {

        final long nodeId = initWithNode( db );

        Thread t1 = newThreadForNodeAction( nodeId, node -> node.addLabel( Label.label( "A" ) ) );

        Thread t2 = newThreadForNodeAction( nodeId, node -> node.addLabel( Label.label( "A" ) ) );

        startAndWait( t1, t2 );

        db.shutdown();

        assertDatabaseConsistent();
    }

    @Test
    public void setConcurrentlySamePropertyWithDifferentValuesOnANode() throws Throwable
    {
        final long nodeId = initWithNode( db );

        Thread t1 = newThreadForNodeAction( nodeId, node -> node.setProperty( "a", 0.788 ) );

        Thread t2 = newThreadForNodeAction( nodeId, node -> node.setProperty( "a", new double[]{0.999, 0.77} ) );

        startAndWait( t1, t2 );

        db.shutdown();

        assertDatabaseConsistent();
    }

    @Test
    public void setConcurrentlySamePropertyWithDifferentValuesOnARelationship() throws Throwable
    {
        final long relId = initWithRel( db );

        Thread t1 = newThreadForRelationshipAction( relId, relationship -> relationship.setProperty( "a", 0.788 ) );

        Thread t2 = newThreadForRelationshipAction( relId,
                relationship -> relationship.setProperty( "a", new double[]{0.999, 0.77} ) );

        startAndWait( t1, t2 );

        db.shutdown();

        assertDatabaseConsistent();
    }

    private long initWithNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node theNode = db.createNode();
            long id = theNode.getId();
            tx.success();
            return id;
        }

    }

    private long initWithRel( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "a", "prop" );
            Relationship rel = node.createRelationshipTo( db.createNode(), RelationshipType.withName( "T" ) );
            long id = rel.getId();
            tx.success();
            return id;
        }
    }

    private Thread newThreadForNodeAction( final long nodeId, final Consumer<Node> nodeConsumer )
    {
        return new Thread( () -> {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( nodeId );
                barrier.await();
                nodeConsumer.accept( node );
                tx.success();
            }
            catch ( Exception e )
            {
                ex.set( e );
            }
        } );
    }

    private Thread newThreadForRelationshipAction( final long relationshipId, final Consumer<Relationship> relConsumer )
    {
        return new Thread( () -> {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship relationship = db.getRelationshipById( relationshipId );
                barrier.await();
                relConsumer.accept( relationship );
                tx.success();
            }
            catch ( Exception e )
            {
                ex.set( e );
            }
        } );
    }

    private void startAndWait( Thread t1, Thread t2 ) throws Exception
    {
        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if ( ex.get() != null )
        {
            throw ex.get();
        }
    }

    private void assertDatabaseConsistent()
    {
        LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        try
        {
            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                    testDirectory.graphDbDir(), Config.defaults(), ProgressMonitorFactory.textual( System.err ),
                    logProvider, false );
            assertTrue( result.isSuccessful() );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            fail( e.getMessage() );
        }
    }
}
