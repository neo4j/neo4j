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

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

    private static abstract class CheckingIterable implements
        Iterable<Transport>
    {
        public Iterator<Transport> iterator()
        {
            try
            {
                final Iterator<?> iterator = provideIterator();
                return new Iterator<Transport>()
                {
                    Transport cached = null;

                    public boolean hasNext()
                    {
                        if ( cached != null )
                        {
                            return true;
                        }
                        else
                            while ( iterator.hasNext() )
                            {
                                try
                                {
                                    cached = ( Transport ) iterator.next();
                                    return true;
                                }
                                // FIXME: be more specific than Throwable
                                // catching all throwables is dangerous,
                                // but only catching exceptions here is wrong.
                                catch ( Throwable ex )
                                {
                                    cached = null;
                                }
                            }
                        return false;
                    }

                    public Transport next()
                    {
                        if ( hasNext() )
                        {
                            try
                            {
                                return cached;
                            }
                            finally
                            {
                                cached = null;
                            }
                        }
                        else
                        {
                            throw new IllegalStateException();
                        }
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            catch ( Exception ex )
            {
                return Arrays.asList( new Transport[ 0 ] ).iterator();
            }
        }

        abstract Iterator<?> provideIterator() throws Exception;
    };

    ProtocolService()
    {
        Iterable<Transport> result;
        // First, try Java 6 API, since it is standardized.
        try
        {
            Class<?> serviceLoaderClass = Class
                .forName( "java.util.ServiceLoader" );
            Method loadMethod = serviceLoaderClass.getMethod( "load",
                Class.class );
            @SuppressWarnings( "unchecked" )
            final Iterable<Transport> iter = ( ( Iterable<Transport> ) loadMethod
                .invoke( null, Transport.class ) );
            result = new CheckingIterable()
            {
                @Override
                Iterator<?> provideIterator()
                {
                    return iter.iterator();
                }
            };
        }
        catch ( Exception ex )
        {
            Iterable<Transport> empty = Arrays
                .asList( new Transport[ 0 ] );
            // If that fails, try the SUN specific Java 5 implementation.
            try
            {
                Class<?> serviceClass = Class.forName( "sun.misc.Service" );
                final Method providersMethod = serviceClass.getMethod(
                    "providers", Class.class );
                result = new CheckingIterable()
                {
                    @Override
                    Iterator<?> provideIterator() throws Exception
                    {
                        return ( Iterator<?> ) providersMethod.invoke( null,
                            Transport.class );
                    }
                };
            }
            catch ( Exception e )
            {
                result = empty;
            }
        }
        factories = result;
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
