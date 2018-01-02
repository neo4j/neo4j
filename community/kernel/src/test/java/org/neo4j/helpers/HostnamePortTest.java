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
package org.neo4j.helpers;

import org.junit.Test;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

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
    public void testGetHostAddress() throws Exception
    {
    	// Given
    	String hostName = InetAddress.getLocalHost().getHostName();
    	
    	// When & Then
    	
    	// should return default, when host is null
    	assertThat( HostnamePort.getHostAddress( null, "default" ), equalTo( "default" ) );
    	
    	// should return host ip address when host is known
        assertThat( HostnamePort.getHostAddress( hostName, "default" ), equalTo( hostName ) );

    }
    
    @Test
    public void testGetHostAddressUnknown() throws Exception
    {
    	// Given
    	String unknownHost = "unknownHost";

    	boolean unknownHostUnknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost );
    	} 
    	catch ( UnknownHostException e )
    	{
    		unknownHostUnknown = true;
    	}    	
    	// this checks if hypothesis is actually true
    	assumeTrue( unknownHostUnknown );

    	// When & Then
    	
    	// should return hostname when it is unknown
    	assertThat( HostnamePort.getHostAddress( unknownHost, "default" ), equalTo( unknownHost ) );    	
    }
    
    @Test
    public void testMatchesUnknownHosts() throws Exception
    {
    	// Given 
        String knownHost = InetAddress.getLocalHost().getHostName();
    	String unknownHost1 = "unknownHost1";
    	String unknownHost2 = "unknownHost2";
    	
    	boolean unknownHost1Unknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost1 );
    	}
    	catch ( UnknownHostException e)
    	{
    		unknownHost1Unknown = true;
    	}

    	boolean unknownHost2Unknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost2 );
    	}
    	catch ( UnknownHostException e)
    	{
    		unknownHost2Unknown = true;
    	}    	
    	// skipping if they're not unknown: test doesn't make sense
    	assumeTrue( unknownHost1Unknown && unknownHost2Unknown);
    	
        HostnamePort hostnamePortSinglePort = new HostnamePort( unknownHost1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( unknownHost1 + ":1234-1236" );

        // When & Then
        
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost1 + ":1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost1 + ":1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost2 + ":1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost2 + ":1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost1 + "" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost2 + "" ) ) );
       
    	// Unknown host should never match with any IP or known host
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://1.2.3.4:1234" ) ) );
        
        // Should return false with any other known host
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + knownHost + ":1234" ) ) );

        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 + ":1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 + ":1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 + ":1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost2 + ":1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost2 + ":1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 + ":1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 + ":1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost1 ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost2 ) ) );

    }

    @Test
    public void testMatchesKnownHostWithIP() throws Exception
    {
    	// Given
    	
    	String hostname1 = InetAddress.getLocalHost().getHostName();
    	String host1 = InetAddress.getLocalHost().getHostAddress();
    	// Building fake IP for host2
    	StringBuilder host2 = new StringBuilder();
    	String [] host1Parts = host1.split( "\\." );
    	for ( String part : host1Parts )
    	{
    		int partnum = Integer.parseInt( part );
    		host2.append( ++partnum % 256 + "." );
    	}
    	host2.deleteCharAt( host2.length() - 1 );
    	
        HostnamePort hostnamePortSinglePort = new HostnamePort( hostname1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( hostname1 + ":1234-1236" );
        
        // When & Then
        
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://" + hostname1 + ":1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + hostname1 + ":1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 + ":1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 + ":1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 ) ) );

        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 + ":1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 + ":1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 + ":1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 + ":1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 + ":1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 + ":1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 + ":1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname1 ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 ) ) );
    }

    @Test
    public void testMatchesIPWithHost() throws Exception
    {
    	// Given 
    	
    	String hostname1 = InetAddress.getLocalHost().getHostName();
    	String host1 = InetAddress.getLocalHost().getHostAddress();
    	String hostname2 = "neo4j.org";
    	
    	boolean host2Known = true;
    	try
    	{
    		InetAddress.getByName( hostname2 );
    	}
    	catch ( UnknownHostException e)
    	{
    		host2Known = false;    		
    	}    	
    	assumeTrue( host2Known );    	
    	assumeFalse( hostname1.equals( hostname2 ) );
  	
        HostnamePort hostnamePortSinglePort = new HostnamePort( host1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( host1 + ":1234-1236" );
        
        // When & Then
        
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 + ":1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 + ":1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + hostname2 + ":1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + hostname2 + ":1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + hostname2 ) ) );

        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname2 + ":1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname2 + ":1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + hostname2 ) ) );
 
    }

    @Test
    public void testMatchesIPWithHostUnknown() throws Exception
    {
    	// Given 
       	String unknownHost = "unknownHost";
    	boolean unknownHostUnknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost );
    	}
    	catch( UnknownHostException e)
    	{
    		unknownHostUnknown = true;
    	}    	
    	assumeTrue( unknownHostUnknown );

       	String host1 = InetAddress.getLocalHost().getHostAddress();
       	
        HostnamePort hostnamePortSinglePort = new HostnamePort( host1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( host1 + ":1234-1236" );
        
    	// When & Then
    	
    	// should return false if matched with any unknown host
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost + ":1234") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1234") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1235") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1236") ) );
    }
    
    @Test
    public void testMatchesKnownHostWithHost() throws Exception
    {
    	// Given
    	
    	String host1 = InetAddress.getLocalHost().getHostName();
    	// any other hostname?
    	String host2 = "neo4j.org";
    	
    	boolean host2Known = true;
    	try
    	{
    		InetAddress.getByName( host2 );
    	}
    	catch ( UnknownHostException e)
    	{
    		host2Known = false;    		
    	}
    	assumeTrue( host2Known );
    	assumeFalse( host1.equals( host2 ) );
    	
    	    	
        HostnamePort hostnamePortSinglePort = new HostnamePort( host1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( host1 + ":1234-1236" );
        
        // When & Then
     
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 + ":1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 + ":1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 + ":1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 + ":1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host2 ) ) );

        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 + ":1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 + ":1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 + ":1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host1 ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + host2 ) ) );
		
    }

    @Test
    public void testMatchesKnownHostWithHostUnknown() throws Exception
    {
    	// Given	   	
    	String host1 = InetAddress.getLocalHost().getHostName();    	
        String unknownHost = "unknownHost";
    	
    	boolean unknownHostUnknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost );
    	}
    	catch ( UnknownHostException e )
    	{
    		unknownHostUnknown = true;
    	}
        assumeTrue( unknownHostUnknown ); 
        
        HostnamePort hostnamePortSinglePort = new HostnamePort( host1 + ":1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( host1 + ":1234-1236" );
        
        // When & Then
        
        // should return false if matched with any unknown host
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost + ":1234") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1234") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1235") ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://" + unknownHost + ":1236") ) );
    }
    
    @Test
    public void testMatchesIP() throws Exception
    {
    	// Given
    	
        HostnamePort hostnamePortSinglePort = new HostnamePort( "1.2.3.4:1234" );
        HostnamePort hostnamePortWithRange = new HostnamePort( "1.2.3.4:1234-1236" );
        
        // When & Then
        
        // Should match, same host and port
        assertTrue( hostnamePortSinglePort.matches( URI.create( "ha://1.2.3.4:1234" ) ) );
        // Should fail, different host or port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://1.2.3.4:1235" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://5.6.7.8:1234" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://5.6.7.8:1235" ) ) );
        // Should fail, no port
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://1.2.3.4" ) ) );
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://5.6.7.8" ) ) );

        // Should match, port in range and host the same
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4:1234" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4:1235" ) ) );
        assertTrue( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4:1236" ) ) );
        // Should not match, different host
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://5.6.7.8:1234" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://5.6.7.8:1235" ) ) );
        // Should not match, port outside of range
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4:1233" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4:1237" ) ) );
        // Should not match, no port
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://1.2.3.4" ) ) );
        assertFalse( hostnamePortWithRange.matches( URI.create( "ha://5.6.7.8" ) ) );
    }
    
    @Test
    public void testMatchesNullHostWithUnknownHost() throws Exception
    {    	
    	// Given
    	
        HostnamePort hostnamePortSinglePort = new HostnamePort( ":1234" );
        String unknownHost = "unknownHost";
        boolean unknownHostUnknown = false;
    	try
    	{
    		InetAddress.getByName( unknownHost );
    	}
    	catch ( UnknownHostException e)
    	{
    		unknownHostUnknown = true;
    	}    	
    	assumeTrue( unknownHostUnknown );
    	
    	// When & Then
    	
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + unknownHost + ":1234" ) ) );
    }

    @Test
    public void testMatchesNullHostWithIP() throws Exception
    {
        HostnamePort hostnamePortSinglePort = new HostnamePort( ":1234" );
        String host1IP = InetAddress.getLocalHost().getHostAddress();
        
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1IP + ":1234" ) ) );
    }

    @Test
    public void testMatchesNullHostWithKnownHost() throws Exception
    {
        HostnamePort hostnamePortSinglePort = new HostnamePort( ":1234" );
        String host1 = InetAddress.getLocalHost().getHostName();
        
        assertFalse( hostnamePortSinglePort.matches( URI.create( "ha://" + host1 + ":1234" ) ) );
    }
    
    @Test
    public void testIPv6Address()
    {
        HostnamePort hostnamePort = new HostnamePort( "[2001:cdba:0:0:0:0:3257:9652]" );

        assertThat( hostnamePort.getHost( null ), equalTo( "[2001:cdba:0:0:0:0:3257:9652]" ) );
        assertThat( hostnamePort.getPort(), equalTo( 0 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[]{0, 0} ) );
    }

    @Test
    public void testIPv6AddressWithSchemeAndPort()
    {
        HostnamePort hostnamePort = new HostnamePort( "foo://[ff02::1:1]:9191" );

        assertThat( hostnamePort.getHost( null ), equalTo( "[ff02::1:1]" ) );
        assertThat( hostnamePort.getPort(), equalTo( 9191 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[]{9191, 9191} ) );
    }

    @Test
    public void testIPv6Localhost()
    {
        HostnamePort hostnamePort = new HostnamePort( "[::1]" );

        assertThat( hostnamePort.getHost( null ), equalTo( "[::1]" ) );
        assertThat( hostnamePort.getPort(), equalTo( 0 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[]{0, 0} ) );
    }

    @Test
    public void testIPv6LocalhostWithSchemeAndPort()
    {
        HostnamePort hostnamePort = new HostnamePort( "foo://[::1]:6362" );

        assertThat( hostnamePort.getHost( null ), equalTo( "[::1]" ) );
        assertThat( hostnamePort.getPort(), equalTo( 6362 ) );
        assertThat( hostnamePort.getPorts(), equalTo( new int[]{6362, 6362} ) );
    }
}
