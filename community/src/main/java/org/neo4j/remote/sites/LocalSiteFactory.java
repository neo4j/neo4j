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
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.api.core.EmbeddedNeo;
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

    private static Map<String, NeoServiceContainer> instances = new HashMap<String, NeoServiceContainer>();

    static synchronized NeoServiceContainer getNeoService( File file )
    {
        String path;
        try
        {
            path = file.getCanonicalPath();
        }
        catch ( IOException ex )
        {
            path = file.getAbsolutePath();
        }
        NeoServiceContainer neo = instances.get( path );
        if ( neo == null )
        {
            neo = new NeoServiceContainer( new EmbeddedNeo( path ) );
            Runtime.getRuntime().addShutdownHook( new Thread( neo ) );
            instances.put( path, neo );
        }
        return neo;
    }
}
