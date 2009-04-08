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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.remote.RemoteSite;
import org.neo4j.remote.RemoteSiteFactory;

/**
 * A {@link RemoteSiteFactory} that creates {@link LocalSite}s.
 * @author Tobias Ivarsson
 */
public final class LocalSiteFactory extends RemoteSiteFactory
{
    /**
     * Create a new {@link RemoteSiteFactory} for the file:// protocol.
     */
    public LocalSiteFactory()
    {
        super( "file" );
    }

    @Override
    protected RemoteSite create( URI resourceUri )
    {
        return new LocalSite( getNeoService( new File( resourceUri
            .getSchemeSpecificPart() ) ) );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        return "file".equals( resourceUri.getScheme() );
    }

    private static Map<File, NeoService> instances = new HashMap<File, NeoService>();

    private static synchronized NeoService getNeoService( File file )
    {
        NeoService neo = instances.get( file );
        if ( neo == null )
        {
            neo = newNeoService( file );
            instances.put( file, neo );
        }
        return neo;
    }

    private static NeoService newNeoService( File file )
    {
        final NeoService neo = new EmbeddedNeo( file.getPath() );
        Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
        {
            public void run()
            {
                neo.shutdown();
            }
        } ) );
        return neo;
    }
}
