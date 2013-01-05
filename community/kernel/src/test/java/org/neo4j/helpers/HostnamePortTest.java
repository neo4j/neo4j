/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

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
}
