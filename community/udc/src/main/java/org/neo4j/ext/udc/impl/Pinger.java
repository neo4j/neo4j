/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.ext.udc.impl;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.helpers.HostnamePort;

import static org.neo4j.ext.udc.UdcConstants.PING;

class Pinger
{
    private final HostnamePort address;
    private final UdcInformationCollector collector;
    private int pingCount;

    Pinger( HostnamePort address, UdcInformationCollector collector )
    {
        this.address = address;
        this.collector = collector;
    }

    void ping() throws IOException
    {
        pingCount++;

        Map<String, String> usageDataMap = collector.getUdcParams();

        StringBuilder uri = new StringBuilder( "http://" + address + "/" + "?" );

        for ( Map.Entry<String,String> entry : usageDataMap.entrySet() )
        {
            uri.append( entry.getKey() );
            uri.append( "=" );
            uri.append( URLEncoder.encode( entry.getValue(), StandardCharsets.UTF_8.name() ) );
            uri.append( "+" );
        }

        uri.append( PING + "=" ).append( pingCount );

        URL url = new URL( uri.toString() );
        URLConnection con = url.openConnection();

        con.setDoInput( true );
        con.setDoOutput( false );
        con.setUseCaches( false );
        con.connect();

        con.getInputStream().close();
    }

    int getPingCount()
    {
        return pingCount;
    }
}
