/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.remote.transports;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.Transport;

/**
 * A {@link Transport} that creates {@link LocalGraphDatabase}s.
 * @author Tobias Ivarsson
 */
public final class LocalTransport extends Transport
{
    /**
     * Create a new {@link Transport} for the file:// protocol.
     */
    public LocalTransport()
    {
        super( "file" );
    }

    @Override
    protected ConnectionTarget create( URI resourceUri )
    {
        return new LocalGraphDatabase( getGraphDbService( new File( resourceUri
            .getSchemeSpecificPart() ) ) );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        return "file".equals( resourceUri.getScheme() );
    }

    private static ConcurrentMap<String, GraphDbContainer> instances = new ConcurrentHashMap<String, GraphDbContainer>();

    static synchronized GraphDbContainer getGraphDbService( File file )
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
        GraphDbContainer graphDb = instances.get( path );
        if ( graphDb == null )
        {
            graphDb = new GraphDbContainer( new EmbeddedGraphDatabase( path ),
                    path, instances );
            Runtime.getRuntime().addShutdownHook( new Thread( graphDb ) );
            instances.put( path, graphDb );
        }
        return graphDb;
    }
}
