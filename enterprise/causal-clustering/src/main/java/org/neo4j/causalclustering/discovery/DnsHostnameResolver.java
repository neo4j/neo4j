/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
