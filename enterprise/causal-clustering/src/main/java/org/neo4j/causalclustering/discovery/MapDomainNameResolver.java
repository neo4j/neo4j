/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
