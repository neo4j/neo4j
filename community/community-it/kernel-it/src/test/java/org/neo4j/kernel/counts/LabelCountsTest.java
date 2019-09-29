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
package org.neo4j.kernel.counts;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

@ImpermanentDbmsExtension
class LabelCountsTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldGetNumberOfNodesWithLabel()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "Foo" ) );
            tx.createNode( label( "Bar" ) );
            tx.createNode( label( "Bar" ) );

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 2, barCount );
    }

    @Test
    void shouldAccountForDeletedNodes()
    {
        // given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode( label( "Foo" ) );
            tx.createNode( label( "Foo" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );

        // then
        assertEquals( 1, fooCount );
    }

    @Test
    void shouldAccountForDeletedNodesWithMultipleLabels()
    {
        // given
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode( label( "Foo" ), label( "Bar" ) );
            tx.createNode( label( "Foo" ) );
            tx.createNode( label( "Bar" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 1, barCount );
    }

    @Test
    void shouldAccountForAddedLabels()
    {
        // given
        Node n1;
        Node n2;
        Node n3;
        try ( Transaction tx = db.beginTx() )
        {
            n1 = tx.createNode( label( "Foo" ) );
            n2 = tx.createNode();
            n3 = tx.createNode();

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( n1.getId() ).addLabel( label( "Bar" ) );
            tx.getNodeById( n2.getId() ).addLabel( label( "Bar" ) );
            tx.getNodeById( n3.getId() ).addLabel( label( "Foo" ) );

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 2, fooCount );
        assertEquals( 2, barCount );
    }

    @Test
    void shouldAccountForRemovedLabels()
    {
        // given
        Node n1;
        Node n2;
        Node n3;
        try ( Transaction tx = db.beginTx() )
        {
            n1 = tx.createNode( label( "Foo" ), label( "Bar" ) );
            n2 = tx.createNode( label( "Bar" ) );
            n3 = tx.createNode( label( "Foo" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( n1.getId() ).removeLabel( label( "Bar" ) );
            tx.getNodeById( n2.getId() ).removeLabel( label( "Bar" ) );
            tx.getNodeById( n3.getId() ).removeLabel( label( "Foo" ) );

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 0, barCount );
    }

    /** Transactional version of {@link #countsForNode(Transaction, Label)} */
    private long numberOfNodesWith( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeCount = countsForNode( tx, label );
            tx.commit();
            return nodeCount;
        }
    }

    /** @param label the label to get the number of nodes of, or {@code null} to get the total number of nodes. */
    private long countsForNode( Transaction tx, Label label )
    {
        KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
        Read read = transaction.dataRead();
        int labelId;
        if ( label == null )
        {
            labelId = ANY_LABEL;
        }
        else
        {
            if ( TokenRead.NO_TOKEN == (labelId = transaction.tokenRead().nodeLabel( label.name() )) )
            {
                return 0;
            }
        }
        return read.countsForNode( labelId );
    }

}
