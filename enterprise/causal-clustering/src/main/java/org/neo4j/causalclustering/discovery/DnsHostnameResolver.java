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
import java.util.HashSet;
import java.util.Set;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class DnsHostnameResolver implements HostnameResolver
{
    private final Log userLog;
    private final Log log;
    private final DomainNameResolver domainNameResolver;

    public DnsHostnameResolver( LogProvider logProvider, LogProvider userLogProvider,
            DomainNameResolver domainNameResolver )
    {
        log = logProvider.getLog( getClass() );
        userLog = userLogProvider.getLog( getClass() );
        this.domainNameResolver = domainNameResolver;
    }

    @Override
    public Collection<AdvertisedSocketAddress> resolve( AdvertisedSocketAddress initialAddress )
    {
        Set<AdvertisedSocketAddress> addresses = new HashSet<>();
        InetAddress[] ipAddresses = new InetAddress[0];
        try
        {
            ipAddresses = domainNameResolver.resolveDomainName( initialAddress.getHostname() );
        }
        catch ( UnknownHostException e )
        {
            log.error( format("Failed to resolve host '%s'", initialAddress.getHostname()), e);
        }

        for ( InetAddress ipAddress : ipAddresses )
        {
            addresses.add( new AdvertisedSocketAddress( ipAddress.getHostAddress(), initialAddress.getPort() ) );
        }

        userLog.info( "Resolved initial host '%s' to %s", initialAddress, addresses );
        return addresses;
    }
}
