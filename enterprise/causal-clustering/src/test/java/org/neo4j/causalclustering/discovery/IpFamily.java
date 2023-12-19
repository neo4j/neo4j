/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery;

public enum IpFamily
{
    // this assumes that localhost resolves to IPv4, can be changed if problematic (behaviour was already in place)
    IPV4( "localhost", "127.0.0.1", "0.0.0.0" ),
    IPV6( "::1", "::1", "::" );

    private String localhostName;
    private String localhostAddress;
    private String wildcardAddress;

    IpFamily( String localhostName, String localhostAddress, String wildcardAddress )
    {
        this.localhostName = localhostName;
        this.localhostAddress = localhostAddress;
        this.wildcardAddress = wildcardAddress;
    }

    public String localhostName()
    {
        return localhostName;
    }

    public String localhostAddress()
    {
        return localhostAddress;
    }

    public String wildcardAddress()
    {
        return wildcardAddress;
    }
}
