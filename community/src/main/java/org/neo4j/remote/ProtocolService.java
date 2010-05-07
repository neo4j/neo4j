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
package org.neo4j.remote;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.commons.Service;

final class ProtocolService
{
    ConnectionTarget get( URI resourceUri )
    {
        return getSiteFactory( resourceUri ).create( resourceUri );
    }

    synchronized void register( Transport factory )
    {
        if ( factory == null )
        {
            throw new IllegalArgumentException(
                "the RemoteSiteFactory may not be null." );
        }
        for ( String protocol : factory.protocols )
        {
            @SuppressWarnings( "hiding" )
            Set<Transport> factories = implementations.get( protocol );
            if ( factories == null )
            {
                factories = new HashSet<Transport>();
                implementations.put( protocol, factories );
            }
            factories.add( factory );
        }
    }

    private final Map<String, Set<Transport>> implementations = new HashMap<String, Set<Transport>>();
    private final Iterable<Transport> factories;

    ProtocolService()
    {
        factories = Service.load( Transport.class );
    }

    private synchronized Transport getSiteFactory( URI resourceUri )
    {
        Transport result = loadSiteFactory( resourceUri );
        if ( result == null )
        {
            for ( Transport factory : factories )
            {
                register( factory );
            }
            result = loadSiteFactory( resourceUri );
        }
        if ( result != null )
        {
            return result;
        }
        throw new RuntimeException(
            "No implementation available to handle resource URI: "
                + resourceUri + "\nSupported protocolls are: " + allProtocols() );
    }

    private Transport loadSiteFactory( URI resourceUri )
    {
        String protocol = resourceUri.getScheme();
        Iterable<Transport> factories = implementations.get( protocol );
        if ( factories == null )
        {
            return null;
        }
        for ( Transport factory : factories )
        {
            try
            {
                if ( factory.handlesUri( resourceUri ) )
                {
                    return factory;
                }
            }
            catch ( Exception ex )
            {
                // Silently drop
            }
        }
        return null;
    }

    private String allProtocols()
    {
        boolean comma = false;
        StringBuilder result = new StringBuilder();
        for ( Iterable<Transport> factories : implementations.values() )
        {
            for ( Transport factory : factories )
            {
                for ( String protocol : factory.protocols )
                {
                    if ( comma )
                    {
                        result.append( ", " );
                    }
                    result.append( protocol );
                    comma = true;
                }
            }
        }
        if ( comma )
        {
            return result.toString();
        }
        else
        {
            return "None!";
        }
    }
}
