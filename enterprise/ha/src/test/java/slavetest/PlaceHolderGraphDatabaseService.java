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
package slavetest;

import java.util.Collection;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.TransactionBuilder;

public class PlaceHolderGraphDatabaseService extends AbstractGraphDatabase
{
    private volatile GraphDatabaseService db;

    public PlaceHolderGraphDatabaseService( String storeDir )
    {
        super( storeDir );
    }

    public void setDb( GraphDatabaseService db )
    {
        this.db = db;
    }
    
    public GraphDatabaseService getDb()
    {
        return db;
    }

    @Override
    public Node createNode()
    {
        return db.createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return db.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return db.getRelationshipById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return db.getReferenceNode();
    }

    @Override
    protected void close()
    {
        db.shutdown();
    }

    @Override
    public Transaction beginTx()
    {
        return db.beginTx();
    }
    
    @Override
    public TransactionBuilder tx()
    {
        return ((AbstractGraphDatabase)db).tx();
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return db.registerTransactionEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return db.unregisterTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return db.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return db.unregisterKernelEventHandler( handler );
    }

    @Override
    public IndexManager index()
    {
        return db.index();
    }

    @Override
    public Config getConfig()
    {
        return ((AbstractGraphDatabase) db).getConfig();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return ( (AbstractGraphDatabase) db ).getManagementBeans( type );
    }
    
    @Override
    public KernelData getKernelData()
    {
        return ((AbstractGraphDatabase)db).getKernelData();
    }
}
