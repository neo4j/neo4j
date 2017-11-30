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
package org.neo4j.kernel.impl.coreapi;

import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

public class StandardNodeActions implements NodeProxy.NodeActions
{
    private final Supplier<Statement> stmt;
    private final Supplier<KernelTransaction> currentTx;
    private final ThrowingAction<RuntimeException> assertInOpenTransaction;
    private final RelationshipProxy.RelationshipActions relationshipActions;
    private final GraphDatabaseService gds;

    public StandardNodeActions( Supplier<Statement> stmt,
                                Supplier<KernelTransaction> currentTx,
                                ThrowingAction<RuntimeException> assertTransactionOpen,
                                RelationshipProxy.RelationshipActions relationshipActions,
                                GraphDatabaseService gds )
    {
        this.stmt = stmt;
        this.currentTx = currentTx;
        this.assertInOpenTransaction = assertTransactionOpen;
        this.relationshipActions = relationshipActions;
        this.gds = gds;
    }

    @Override
    public Statement statement()
    {
        return stmt.get();
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return currentTx.get();
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return gds;
    }

    @Override
    public void assertInUnterminatedTransaction()
    {
        assertInOpenTransaction.apply();
    }

    @Override
    public void failTransaction()
    {
        currentTx.get().failure();
    }

    @Override
    public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipProxy( relationshipActions, id, startNodeId, typeId, endNodeId );
    }
}
