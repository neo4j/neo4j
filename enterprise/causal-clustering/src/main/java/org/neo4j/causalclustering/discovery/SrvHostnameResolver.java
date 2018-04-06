/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.NamingException;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SrvHostnameResolver implements HostnameResolver
{
    private final Log userLog;
    private final Log log;
    private final SrvRecordResolver srvRecordResolver;

    public SrvHostnameResolver( LogProvider logProvider, LogProvider userLogProvider,
            SrvRecordResolver srvRecordResolver )
    {
        log = logProvider.getLog( getClass() );
        userLog = userLogProvider.getLog( getClass() );
        this.srvRecordResolver = srvRecordResolver;
    }

    @Override
    public Collection<AdvertisedSocketAddress> resolve( AdvertisedSocketAddress initialAddress )
    {
        try
        {
            Set<AdvertisedSocketAddress> addresses = srvRecordResolver
                    .resolveSrvRecord( initialAddress.getHostname() )
                    .map( srvRecord -> new AdvertisedSocketAddress( srvRecord.host, srvRecord.port ) )
                    .collect( Collectors.toSet() );

            userLog.info( "Resolved initial host '%s' to %s", initialAddress, addresses );

            if ( addresses.isEmpty() )
            {
                log.error( "Failed to resolve srv records for '%s'", initialAddress.getHostname() );
            }

            return addresses;
        }
        catch ( NamingException e )
        {
            log.error( "Failed to resolve srv records for '%s'", initialAddress.getHostname(), e );
            return Collections.emptySet();
        }
    }
}
