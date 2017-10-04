/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class MapDomainNameResolver implements DomainNameResolver
{
    private final Map<String /*hostname*/,InetAddress[] /*response*/> domainNameMapping;

    public MapDomainNameResolver( Map<String,InetAddress[]> mapping )
    {
        domainNameMapping = mapping;
    }

    @Override
    public InetAddress[] resolveDomainName( String hostname ) throws UnknownHostException
    {
        if ( domainNameMapping.containsKey( hostname ) )
        {
            return domainNameMapping.get( hostname );
        }
        throw new UnknownHostException( new java.net.UnknownHostException() );
    }

    public void setHostnameAddresses( String hostname, Collection<String> addresses )
    {
        InetAddress[] processedAddresses = new InetAddress[addresses.size()];
        addresses.stream().map( MapDomainNameResolver::inetAddress ).collect( Collectors.toList() )
                .toArray( processedAddresses );
        domainNameMapping.put( hostname, processedAddresses );
    }

    private static InetAddress inetAddress( String address )
    {
        try
        {
            return InetAddress.getByName( address );
        }
        catch ( java.net.UnknownHostException e )
        {
            throw new UnknownHostException( e );
        }
    }
}
