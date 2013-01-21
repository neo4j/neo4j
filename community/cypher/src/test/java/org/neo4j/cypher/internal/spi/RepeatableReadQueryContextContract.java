/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.internal.spi.gdsimpl.RepeatableReadQueryContext;
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class RepeatableReadQueryContextContract
{
    private ImpermanentGraphDatabase database;
    private QueryContext innerContext;
    private Node node;
    private RepeatableReadQueryContext.Locker locker;

    @Before
    public void init()
    {
        database = new ImpermanentGraphDatabase();
        locker = mock( RepeatableReadQueryContext.Locker.class );
        innerContext = new TransactionBoundQueryContext( database );
        Transaction tx = database.beginTx();
        node = database.createNode();
        Node b = database.createNode();
        Node c = database.createNode();
        node.createRelationshipTo( b, withName( "R" ) );
        node.createRelationshipTo( c, withName( "R" ) );
        tx.success();
        tx.finish();
    }

    @Test
    public void has_property_locks_node() throws Exception
    {
        // Given
        Node node = createNode();
        RepeatableReadQueryContext lockingContext = new RepeatableReadQueryContext( innerContext, locker );

        //When
        lockingContext.nodeOps().hasProperty( node, "foo" );

        //Then
        verify( locker ).readLock( node );
    }

    @Test
    public void close_releases_locks() throws Exception
    {
        // Given
        Node node = createNode();
        RepeatableReadQueryContext lockingContext = new RepeatableReadQueryContext( innerContext, locker );

        //When
        lockingContext.nodeOps().hasProperty( node, "foo" );
        lockingContext.close();

        //Then
        verify( locker ).readLock( node );
        verify( locker ).releaseAllReadLocks();
    }

    @Test
    public void get_relationships_locks_node_and_relationships() throws Exception
    {
        // Given
        RepeatableReadQueryContext lockingContext = new RepeatableReadQueryContext( innerContext, locker );

        //When
        Iterable<Relationship> rels = lockingContext.getRelationshipsFor( node, Direction.OUTGOING );
        int count_the_matching_rows = count( rels );
        lockingContext.close();

        //Then
        verify( locker ).readLock( node );
        for ( Relationship rel : rels )
        {
            //Relationship locked
            verify( locker ).readLock( rel );
        }
        verify( locker ).releaseAllReadLocks();

        assertThat( count_the_matching_rows, is( 2 ) );
    }

    private Node createNode()
    {
        Transaction tx = database.beginTx();
        Node node = database.createNode();
        tx.success();
        tx.finish();
        return node;
    }
}
