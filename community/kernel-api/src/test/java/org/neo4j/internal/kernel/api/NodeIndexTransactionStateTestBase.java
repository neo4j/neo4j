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
package org.neo4j.internal.kernel.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public abstract class NodeIndexTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPerformStringSuffixSearch() throws Exception
    {
        // given
        PrimitiveLongSet expected = Primitive.longSet();
        try ( Transaction tx = session.beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "1suff" ) );
            nodeWithProp( tx, "pluff" );
            tx.success();
        }

        createIndex();

        // when
        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "2suff" ) );
            nodeWithProp( tx, "skruff" );
            CapableIndexReference index = tx.schemaRead().index( label, prop );
            try ( NodeValueIndexCursor nodes = tx.cursors().allocateNodeValueIndexCursor() )
            {
                tx.dataRead().nodeIndexSeek( index, nodes, IndexOrder.NONE, IndexQuery.stringSuffix( prop, "suff" ) );
                PrimitiveLongSet found = Primitive.longSet();
                while ( nodes.next() )
                {
                    found.add( nodes.nodeReference() );
                }

                assertThat( found, equalTo( expected ) );
            }
        }
    }

    @Test
    public void shouldPerformStringContainsSearch() throws Exception
    {
        // given
        PrimitiveLongSet expected = Primitive.longSet();
        try ( Transaction tx = session.beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "gnomebat" ) );
            nodeWithProp( tx, "fishwombat" );
            tx.success();
        }

        createIndex();

        // when
        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "homeopatic" ) );
            nodeWithProp( tx, "telephonecompany" );
            CapableIndexReference index = tx.schemaRead().index( label, prop );
            try ( NodeValueIndexCursor nodes = tx.cursors().allocateNodeValueIndexCursor() )
            {
                tx.dataRead().nodeIndexSeek( index, nodes, IndexOrder.NONE, IndexQuery.stringContains( prop, "me" ) );
                PrimitiveLongSet found = Primitive.longSet();
                while ( nodes.next() )
                {
                    found.add( nodes.nodeReference() );
                }

                assertThat( found, equalTo( expected ) );
            }
        }
    }

    @Test
    public void shouldThrowIfTransactionTerminated() throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            // given
            terminate( tx );

            // expect
            exception.expect( TransactionTerminatedException.class );

            // when
            tx.dataRead().nodeExists( 42 );
        }
    }

    protected abstract void terminate( Transaction transaction );

    private long nodeWithProp( Transaction tx, Object value ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( "Node" ) );
        write.nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ), Values.of( value ) );
        return node;
    }

    private void createIndex()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( Label.label( "Node" ) ).on( "prop" ).create();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }
}
