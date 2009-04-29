/*
 * Copyright 2008-2009 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote.sites;

import java.io.File;

import javax.transaction.TransactionManager;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.remote.BasicNeoServer;
import org.neo4j.remote.RemoteSite;

/**
 * An implementation of {@link RemoteSite} that isn't really remote. This
 * implementation is useful for implementing servers for other
 * {@link RemoteSite}s and for testing purposes.
 * @author Tobias Ivarsson
 */
public final class LocalSite extends BasicNeoServer
{
    private final NeoService neo;

    /**
     * Create a new local {@link RemoteSite}.
     * @param neo
     *            The {@link NeoService} to connect to with this site.
     */
    public LocalSite( NeoService neo )
    {
        super( getTransactionManagerFor( neo ) );
        this.neo = neo;
    }
    
    /**
     * Create a new local {@link RemoteSite}.
     * @param path
     *            The path to the Neo store.
     */
    public LocalSite( String path )
    {
    	this( LocalSiteFactory.getNeoService( new File( path ) ) );
    }

    @Override
    protected NeoService connectNeo()
    {
        return neo;
    }

    @Override
    protected NeoService connectNeo( String username, String password )
    {
        return neo;
    }

    private static TransactionManager getTransactionManagerFor( NeoService neo )
    {
        if ( neo instanceof EmbeddedNeo )
        {
            return ( ( EmbeddedNeo ) neo ).getConfig().getTxModule()
                .getTxManager();
        }
        else
        {
            throw new IllegalArgumentException(
                "Cannot get transaction manager from neo instance of class="
                    + neo.getClass() );
        }
    }
}
