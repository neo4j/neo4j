/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class HostnamePortTest
{
    @Test
    public void testHostnameOnly()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost" );
        Assert.assertThat( hostnamePort.getHost(), CoreMatchers.equalTo( "myhost" ) );
        Assert.assertThat( hostnamePort.getPort(), CoreMatchers.equalTo( 0 ) );
        Assert.assertThat( hostnamePort.getPorts(), CoreMatchers.equalTo( new int[]{0, 0} ) );
    }

    @Test
    public void testHostnamePort()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1234" );
        Assert.assertThat( hostnamePort.getHost(), CoreMatchers.equalTo( "myhost" ) );
        Assert.assertThat( hostnamePort.getPort(), CoreMatchers.equalTo( 1234 ) );
        Assert.assertThat( hostnamePort.getPorts(), CoreMatchers.equalTo( new int[] {1234, 1234} ) );
    }

    @Test
    public void testHostnamePortRange()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1234-1243" );
        Assert.assertThat( hostnamePort.getHost(), CoreMatchers.equalTo( "myhost" ) );
        Assert.assertThat( hostnamePort.getPort(), CoreMatchers.equalTo( 1234 ) );
        Assert.assertThat( hostnamePort.getPorts(), CoreMatchers.equalTo( new int[] {1234, 1243} ) );
    }

    @Test
    public void testHostnamePortRangeInversed()
    {
        HostnamePort hostnamePort = new HostnamePort( "myhost:1243-1234" );
        Assert.assertThat( hostnamePort.getHost(), CoreMatchers.equalTo( "myhost" ) );
        Assert.assertThat( hostnamePort.getPort(), CoreMatchers.equalTo( 1243 ) );
        Assert.assertThat( hostnamePort.getPorts(), CoreMatchers.equalTo( new int[] {1243, 1234} ) );
    }
}
