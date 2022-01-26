/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.security;

import inet.ipaddr.IPAddressString;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.security.URLAccessValidationError;

class WebURLAccessRule implements URLAccessRule
{
    @Override
    public URL validate( Configuration config, URL url ) throws URLAccessValidationError
    {
        List<IPAddressString> blockedIpRanges = config.get( GraphDatabaseInternalSettings.cypher_ip_blocklist );
        String host = url.getHost();
        if ( !blockedIpRanges.isEmpty() && host != null && !host.isEmpty() )
        {
            try
            {
                InetAddress inetAddress = InetAddress.getByName( url.getHost() );

                for ( var blockedIpRange : blockedIpRanges )
                {
                    if ( blockedIpRange.contains( new IPAddressString( inetAddress.getHostAddress() ) ) )
                    {
                        throw new URLAccessValidationError( "access to " + inetAddress + " is blocked via the configuration property "
                                                            + GraphDatabaseInternalSettings.cypher_ip_blocklist.name() );
                    }
                }
            }
            catch ( UnknownHostException e )
            {
                throw new URLAccessValidationError( "Unable to verify access to " + host + ". Cause: " + e.getMessage() );
            }
        }
        return url;
    }
}
