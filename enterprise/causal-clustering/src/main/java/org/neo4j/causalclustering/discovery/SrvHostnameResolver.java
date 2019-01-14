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
