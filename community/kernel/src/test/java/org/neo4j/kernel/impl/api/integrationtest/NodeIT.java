/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.junit.Assert.assertEquals;

public class NodeIT  extends KernelIntegrationTest
{
    @Test
    public void nodeScanShouldBehaveTheSameUsingIteratorsAndCursors() throws Throwable
    {
        createNodesAndCommit( 100 );

        ReadOperations read = readOperationsInNewTransaction();

        PrimitiveLongSet cursorIds = Primitive.longSet();
        read.nodeGetAllCursor().forAll( n -> cursorIds.add( n.id() ) );

        PrimitiveLongSet iteratorIds = PrimitiveLongCollections.asSet( read.nodesGetAll() );

        commit();

        assertEquals( iteratorIds, cursorIds );
    }

    @Test
    public void nodeScanShouldBehaveTheSameUsingIteratorsAndCursorsWithAddedNodesInTxState() throws Throwable
    {
        createNodesAndCommit( 100 );
        Statement statement = statementInNewTransaction( AnonymousContext.write() );
        createNodes( statement.dataWriteOperations(), 20 );

        PrimitiveLongSet cursorIds = Primitive.longSet();
        statement.readOperations().nodeGetAllCursor().forAll( n -> cursorIds.add( n.id() ) );

        PrimitiveLongSet iteratorIds = PrimitiveLongCollections.asSet( statement.readOperations().nodesGetAll() );

        commit();

        assertEquals( iteratorIds, cursorIds );
    }

    @Test
    public void nodeScanShouldBehaveTheSameUsingIteratorsAndCursorsWithAddedAndDeletedNodesInTxState() throws Throwable
    {
        createNodesAndCommit( 100 );
        deleteNodesAndCommit( 24, 56 );
        Statement statement = statementInNewTransaction( AnonymousContext.write() );
        createNodes( statement.dataWriteOperations(), 20 );
        deleteNodes( statement.dataWriteOperations(), 102, 33, 44 );

        PrimitiveLongSet cursorIds = Primitive.longSet();
        statement.readOperations().nodeGetAllCursor().forAll( n -> cursorIds.add( n.id() ) );

        PrimitiveLongSet iteratorIds = PrimitiveLongCollections.asSet( statement.readOperations().nodesGetAll() );

        commit();

        assertEquals( iteratorIds, cursorIds );
    }

    @Test
    public void nodeCursorShouldBeStable() throws Throwable
    {
        createNodesAndCommit( 100 );
        deleteNodesAndCommit( 24, 56 );
        Statement statement = statementInNewTransaction( AnonymousContext.write() );
        createNodes( statement.dataWriteOperations(), 20 );
        deleteNodes( statement.dataWriteOperations(), 102, 33, 44 );

        statement.readOperations().nodeGetAllCursor().forAll( n ->
        {
            try
            {
                statement.dataWriteOperations().nodeCreate();
            }
            catch ( InvalidTransactionTypeKernelException e )
            {
                // ignore for this test
            }
        } );

        commit();
    }

    private void deleteNodesAndCommit( long... nodeIds ) throws KernelException
    {
        deleteNodes( dataWriteOperationsInNewTransaction(), nodeIds );
        commit();
    }

    private void deleteNodes( DataWriteOperations write, long... nodeIds ) throws KernelException
    {
        for ( long nodeId : nodeIds )
        {
            write.nodeDelete( nodeId );
        }
    }

    private void createNodesAndCommit( int nodes ) throws KernelException
    {
        createNodes( dataWriteOperationsInNewTransaction(), nodes );
        commit();
    }

    private void createNodes( DataWriteOperations write, int nodes ) throws KernelException
    {
        for ( int i = 0; i < nodes; i++ )
        {
            write.nodeCreate();
        }
    }
}
