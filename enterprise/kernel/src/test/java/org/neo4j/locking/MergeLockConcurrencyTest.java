/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.locking;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ConfigBuilder;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.lock_manager;
import static org.neo4j.test.ConfigBuilder.configure;
import static org.neo4j.test.rule.concurrent.ThreadingRule.await;

@RunWith( Parameterized.class )
public class MergeLockConcurrencyTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> configurations()
    {
        return Arrays.asList(
                configure( lock_manager, "community" ).asParameters(),
                configure( lock_manager, "forseti" ).asParameters()
        );
    }

    public MergeLockConcurrencyTest( ConfigBuilder config )
    {
        db.withConfiguration( config.configuration() );
    }

    @Test
    public void shouldNotDeadlockOnMergeFollowedByPropertyAssignment() throws Exception
    {
        withConstraint( mergeThen( this::reassignProperties ) );
    }

    @Test
    public void shouldNotDeadlockOnMergeFollowedByLabelReAddition() throws Exception
    {
        withConstraint( mergeThen( this::reassignLabels ) );
    }

    private void withConstraint( ThrowingFunction<CyclicBarrier,Node,Exception> action ) throws Exception
    {
        // given
        db.execute( "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.bar IS UNIQUE" );
        CyclicBarrier barrier = new CyclicBarrier( 2 );
        Node node = mergeNode();

        // when
        List<Node> result = await( threads.multiple( barrier.getParties(), action, barrier ) );

        // then
        assertEquals( "size of result", 2, result.size() );
        assertEquals( node, result.get( 0 ) );
        assertEquals( node, result.get( 1 ) );
    }

    private ThrowingFunction<CyclicBarrier,Node,Exception> mergeThen(
            ThrowingConsumer<Node,? extends Exception> action )
    {
        return barrier ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = mergeNode();

                barrier.await();

                action.accept( node );

                tx.success();
                return node;
            }
        };
    }

    private Node mergeNode()
    {
        return (Node) single( db.execute( "MERGE (foo:Foo{bar:'baz'}) RETURN foo" ) ).get( "foo" );
    }

    private void reassignProperties( Node node )
    {
        for ( Map.Entry<String,Object> property : node.getAllProperties().entrySet() )
        {
            node.setProperty( property.getKey(), property.getValue() );
        }
    }

    private void reassignLabels( Node node )
    {
        for ( Label label : node.getLabels() )
        {
            node.addLabel( label );
        }
    }
}
