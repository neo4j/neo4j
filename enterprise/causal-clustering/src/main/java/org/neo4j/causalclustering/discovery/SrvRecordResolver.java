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

import java.util.stream.Stream;
import javax.naming.NamingException;

public abstract class SrvRecordResolver
{
    public abstract Stream<SrvRecord> resolveSrvRecord( String url ) throws NamingException;

    public Stream<SrvRecord> resolveSrvRecord( String service, String protocol, String hostname ) throws NamingException
    {
        String url = String.format( "_%s._%s.%s", service, protocol, hostname );

        return resolveSrvRecord( url );
    }

    public static class SrvRecord
    {
        public final int priority;
        public final int weight;
        public final int port;
        public final String host;

        private SrvRecord( int priority, int weight, int port, String host )
        {
            this.priority = priority;
            this.weight = weight;
            this.port = port;
            // Typically the SRV record has a trailing dot - if that is the case we should remove it
            this.host = host.charAt( host.length() - 1 ) == '.' ? host.substring( 0, host.length() - 1 ) : host;
        }

        public static SrvRecord parse( String input )
        {
            String[] parts = input.split( " " );
            return new SrvRecord(
                    Integer.parseInt( parts[0] ),
                    Integer.parseInt( parts[1] ),
                    Integer.parseInt( parts[2] ),
                    parts[3]
            );
        }
    }
}
