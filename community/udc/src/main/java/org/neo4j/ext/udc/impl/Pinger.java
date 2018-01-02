/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.Map;

import org.neo4j.helpers.HostnamePort;

import static org.neo4j.ext.udc.UdcConstants.PING;

public class Pinger
{
    private final HostnamePort address;
    private final UdcInformationCollector collector;
    private int pingCount = 0;

    public Pinger( HostnamePort address, UdcInformationCollector collector )
    {
        this.address = address;
        this.collector = collector;
        if ( collector.getCrashPing() )
        {
            pingCount = -1;
        }
    }


    public void ping() throws IOException
    {
        pingCount++;

        Map<String, String> usageDataMap = collector.getUdcParams();

        StringBuilder uri = new StringBuilder( "http://" + address + "/" + "?" );

        for ( String key : usageDataMap.keySet() )
        {
            uri.append( key );
            uri.append( "=" );
            uri.append( URLEncoder.encode( usageDataMap.get( key ), "UTF-8") );
            uri.append( "+" );
        }

        // append counts
        if ( pingCount == 0 )
        {
            uri.append( PING + "=-1" );
            pingCount++;
        }
        else
        {
            uri.append( PING + "=" ).append( pingCount );
        }

        URL url = new URL( uri.toString() );
        URLConnection con = url.openConnection();

        con.setDoInput( true );
        con.setDoOutput( false );
        con.setUseCaches( false );
        con.connect();

        con.getInputStream().close();
    }

    public Integer getPingCount()
    {
        return pingCount;
    }
}