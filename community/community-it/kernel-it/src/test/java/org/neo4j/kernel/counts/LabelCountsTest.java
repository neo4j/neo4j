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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
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

    private Supplier<KernelTransaction> transactionSupplier;

    @BeforeEach
    void exposeGuts()
    {
        transactionSupplier = () -> db.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true, db.databaseId() );
    }

    @Test
    void shouldGetNumberOfNodesWithLabel()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "Foo" ) );
            db.createNode( label( "Bar" ) );
            db.createNode( label( "Bar" ) );

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
            node = db.createNode( label( "Foo" ) );
            db.createNode( label( "Foo" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();

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
            node = db.createNode( label( "Foo" ), label( "Bar" ) );
            db.createNode( label( "Foo" ) );
            db.createNode( label( "Bar" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();

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
            n1 = db.createNode( label( "Foo" ) );
            n2 = db.createNode();
            n3 = db.createNode();

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            n1.addLabel( label( "Bar" ) );
            n2.addLabel( label( "Bar" ) );
            n3.addLabel( label( "Foo" ) );

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
            n1 = db.createNode( label( "Foo" ), label( "Bar" ) );
            n2 = db.createNode( label( "Bar" ) );
            n3 = db.createNode( label( "Foo" ) );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            n1.removeLabel( label( "Bar" ) );
            n2.removeLabel( label( "Bar" ) );
            n3.removeLabel( label( "Foo" ) );

            tx.commit();
        }

        // when
        long fooCount = numberOfNodesWith( label( "Foo" ) );
        long barCount = numberOfNodesWith( label( "Bar" ) );

        // then
        assertEquals( 1, fooCount );
        assertEquals( 0, barCount );
    }

    /** Transactional version of {@link #countsForNode(Label)} */
    private long numberOfNodesWith( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeCount = countsForNode( label );
            tx.commit();
            return nodeCount;
        }
    }

    /** @param label the label to get the number of nodes of, or {@code null} to get the total number of nodes. */
    private long countsForNode( Label label )
    {
        KernelTransaction transaction = transactionSupplier.get();
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
