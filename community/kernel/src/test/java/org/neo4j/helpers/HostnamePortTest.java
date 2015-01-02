/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers;

import java.net.InetAddress;
import java.net.URI;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HostnamePortTest
{
    @Test
    public void testHostnameOnly()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost" );
        assertThat( hostnamePort.getHost(), equalTo( "myhost" ) );
        assertThat( hostnamePort.getPort(), equalTo( 0 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[]{0, 0} ) );
    }

    @Test
    public void testHostnamePort()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1234" );
        assertThat( hostnamePort.getHost(), equalTo( "myhost" ) );
        assertThat( hostnamePort.getPort(), equalTo( 1234 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[] {1234, 1234} ) );
    }

    @Test
    public void testHostnamePortRange()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1234-1243" );
        assertThat( hostnamePort.getHost(), equalTo( "myhost" ) );
        assertThat( hostnamePort.getPort(), equalTo( 1234 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[] {1234, 1243} ) );
    }

    @Test
    public void testHostnamePortRangeInversed()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1243-1234" );
        assertThat( hostnamePort.getHost(), equalTo( "myhost" ) );
        assertThat( hostnamePort.getPort(), equalTo( 1243 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[] {1243, 1234} ) );
    }
    
    @Test
    public void testSinglePortOnly() throws Exception
    {
        HostnamePort hostnamePort = new HostnamePort( ":1234" );
        assertNull( hostnamePort.getHost() );
        assertThat( hostnamePort.getPort(), equalTo( 1234 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[] { 1234, 1234 } ) );
    }

    @Test
    public void testPortRangeOnly() throws Exception
    {
        HostnamePort hostnamePort = new HostnamePort( ":1230-1240" );
        assertNull( hostnamePort.getHost() );
        assertThat( hostnamePort.getPort(), equalTo( 1230 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[] { 1230, 1240 } ) );
    }

    @Test
    public void testDefaultHost() throws Exception
    {
        HostnamePort hostnamePort = new HostnamePort( ":1234" );
        assertThat( hostnamePort.getHost( "1.2.3.4" ), equalTo( "1.2.3.4" ) );
    }

    @Test
    public void testMatches() throws Exception
    {
        HostnamePort hostnamePortSinglePort = new HostnamePort( "host1:1234" );
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://host1:1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host1:1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host2:1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host2:1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host1" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host2" ) ) );

        HostnamePort hostnamePortWithRange = new HostnamePort( "host1:1234-1236" );
        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://host1:1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://host1:1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://host1:1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host2:1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host2:1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host1:1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host1:1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host1" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://host2" ) ) );
    }

    @Test
    public void testMatchesNullHost() throws Exception
    {
        HostnamePort hostnamePortSinglePort = new HostnamePort( ":1234" );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://host1:1234" ) ) );
        // no scheme means no ports and no host, so both null therefore comparison fails
        assertFalse( hostnamePortSinglePort.matches( URI.create( "host1:1234" ) ) );
    }

    @Test
    public void testHostnameLookup() throws Exception
    {
        String hostName = InetAddress.getLocalHost().getHostName();
        HostnamePort hostnamePort = new HostnamePort( hostName, 1234 );
        assertThat( hostnamePort.toString( null ), equalTo( InetAddress.getByName( hostName ).getHostAddress()+":1234" ) );
    }
}
