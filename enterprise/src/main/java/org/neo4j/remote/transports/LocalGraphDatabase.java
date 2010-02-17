/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.transports;

import java.io.File;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.ConnectionTarget;
import org.neo4j.index.IndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * An implementation of {@link ConnectionTarget} that isn't really remote. This
 * implementation is useful for implementing servers for other
 * {@link ConnectionTarget}s and for testing purposes.
 * @author Tobias Ivarsson
 */
public final class LocalGraphDatabase extends BasicGraphDatabaseServer
{
    /**
     * Create a new local {@link ConnectionTarget}.
     * @param graphDb
     *            The {@link GraphDatabaseService} to connect to with this site.
     */
    public LocalGraphDatabase( GraphDatabaseService graphDb )
    {
        this( new GraphDbContainer( graphDb ) );
    }

    /**
     * Create a new local {@link ConnectionTarget}.
     * @param path
     *            The path to the Neo4j graph database store.
     */
    public LocalGraphDatabase( String path )
    {
        this( LocalTransport.getGraphDbService( new File( path ) ) );
    }

    final GraphDbContainer container;

    LocalGraphDatabase( GraphDbContainer graphDb )
    {
        super( getTransactionManagerFor( graphDb.service ) );
        this.container = graphDb;
    }

    @Override
    protected GraphDatabaseService connectGraphDatabase()
    {
        return container.service;
    }

    @Override
    protected GraphDatabaseService connectGraphDatabase( String username, String password )
    {
        return container.service;
    }

    @Override
    public void registerIndexService( String name, IndexService index )
    {
        super.registerIndexService( name, index );
        container.addIndexService( index );
    }

    private static TransactionManager getTransactionManagerFor( GraphDatabaseService graphDb )
    {
        if ( graphDb instanceof EmbeddedGraphDatabase )
        {
            return ( ( EmbeddedGraphDatabase ) graphDb ).getConfig().getTxModule()
                .getTxManager();
        }
        else
        {
            throw new IllegalArgumentException(
                "Cannot get transaction manager from graph database instance of class="
                    + graphDb.getClass() );
        }
    }
}
