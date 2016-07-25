/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi;

import java.util.function.LongFunction;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipActions;

public class StandardRelationshipActions implements RelationshipActions
{
    private final Supplier<Statement> stmt;
    private final Supplier<KernelTransaction> currentTransaction;
    private final ThrowingAction<RuntimeException> assertInOpenTransaction;
    private final LongFunction<Node> newNodeProxy;
    private final GraphDatabaseService gds;

    public StandardRelationshipActions( Supplier<Statement> stmt, Supplier<KernelTransaction> currentTransaction,
                                        ThrowingAction<RuntimeException> assertInOpenTransaction,
                                        LongFunction<Node> newNodeProxy, GraphDatabaseService gds )
    {
        this.stmt = stmt;
        this.currentTransaction = currentTransaction;
        this.assertInOpenTransaction = assertInOpenTransaction;
        this.newNodeProxy = newNodeProxy;
        this.gds = gds;
    }

    @Override
    public Statement statement()
    {
        return stmt.get();
    }

    @Override
    public Node newNodeProxy( long nodeId )
    {
        return newNodeProxy.apply( nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        try
        {
            return RelationshipType.withName( statement().readOperations().relationshipTypeGetName( type ) );
        }
        catch ( RelationshipTypeIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + type );
        }
    }

    @Override
    public GraphDatabaseService getGraphDatabaseService()
    {
        return gds;
    }

    @Override
    public void failTransaction()
    {
        currentTransaction.get().failure();
    }

    @Override
    public void assertInUnterminatedTransaction()
    {
        assertInOpenTransaction.apply();
    }
}
