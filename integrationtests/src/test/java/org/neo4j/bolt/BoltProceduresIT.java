/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.bolt;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BoltProceduresIT
{
    @ClassRule
    public static final Neo4jRule db = new Neo4jRule()
            .withProcedure( BoltTestProcedures.class )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withConfig( ServerSettings.script_enabled, Settings.TRUE );

    private static Driver driver;

    @BeforeClass
    public static void setUp() throws Exception
    {
        driver = GraphDatabase.driver( db.boltURI() );
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    /**
     * Test creates a situation where streaming of a node fails when accessing node labels/properties.
     * It fails because transaction is terminated. Bolt server should not send half-written message.
     * Driver should receive a regular FAILURE message saying that transaction has been terminated.
     */
    @Test
    public void shouldTransmitStreamingFailure()
    {
        try ( Session session = driver.session() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "name1", randomLongString() );
            params.put( "name2", randomLongString() );
            session.run( "CREATE (n1 :Person {name: $name1}), (n2 :Person {name: $name2}) RETURN n1, n2", params ).consume();

            StatementResult result = session.run( "CALL test.readNodesReturnThemAndTerminateTheTransaction() YIELD node" );

            assertTrue( result.hasNext() );
            Record record = result.next();
            assertEquals( "Person", Iterables.single( record.get( 0 ).asNode().labels() ) );
            assertNotNull( record.get( 0 ).asNode().get( "name" ) );

            try
            {
                result.hasNext();
                fail( "Exception expected" );
            }
            catch ( TransientException e )
            {
                assertEquals( Status.Transaction.Terminated.code().serialize(), e.code() );
            }
        }
    }

    private static String randomLongString()
    {
        return RandomStringUtils.randomAlphanumeric( 10_000 );
    }

    public static class BoltTestProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public KernelTransaction tx;

        @Procedure( name = "test.readNodesReturnThemAndTerminateTheTransaction", mode = Mode.READ )
        public Stream<NodeResult> readNodesReturnThemAndTerminateTheTransaction()
        {
            Result result = db.execute( "MATCH (n) RETURN n" );

            NodeResult[] results = result.stream()
                    .map( record -> (Node) record.get( "n" ) )
                    .map( NodeResult::new )
                    .toArray( NodeResult[]::new );

            return Iterators.stream( new TransactionTerminatingIterator<>( tx, results ) );
        }
    }

    public static class NodeResult
    {
        public Node node;

        NodeResult( Node node )
        {
            this.node = node;
        }
    }

    /**
     * Returnes given elements, terminates the transaction before returning the very last one.
     *
     * @param <T> type of elements.
     */
    private static class TransactionTerminatingIterator<T> implements Iterator<T>
    {
        final KernelTransaction tx;
        final Queue<T> elements;

        @SafeVarargs
        private TransactionTerminatingIterator( KernelTransaction tx, T... elements )
        {
            this.tx = tx;
            this.elements = new ArrayDeque<>();
            Collections.addAll( this.elements, elements );
        }

        @Override
        public boolean hasNext()
        {
            return !elements.isEmpty();
        }

        @Override
        public T next()
        {
            if ( elements.size() == 1 )
            {
                // terminate transaction before returning the last element
                tx.markForTermination( Status.Transaction.Terminated );
            }
            T element = elements.poll();
            if ( element == null )
            {
                throw new NoSuchElementException();
            }
            return element;
        }
    }
}
