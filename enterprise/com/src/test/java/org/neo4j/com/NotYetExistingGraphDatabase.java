/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;

public class NotYetExistingGraphDatabase extends AbstractGraphDatabase
{
    private final String storeDir;

    public NotYetExistingGraphDatabase( String storeDir )
    {
        this.storeDir = storeDir;
    }

    public Node createNode()
    {
        throw new UnsupportedOperationException();
    }

    public Node getNodeById( long id )
    {
        throw new UnsupportedOperationException();
    }

    public Relationship getRelationshipById( long id )
    {
        throw new UnsupportedOperationException();
    }

    public Node getReferenceNode()
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<Node> getAllNodes()
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        throw new UnsupportedOperationException();
    }

    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    public Transaction beginTx()
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public IndexManager index()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStoreDir()
    {
        return storeDir;
    }

    @Override
    public Config getConfig()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly()
    {
        throw new UnsupportedOperationException();
    }
}
