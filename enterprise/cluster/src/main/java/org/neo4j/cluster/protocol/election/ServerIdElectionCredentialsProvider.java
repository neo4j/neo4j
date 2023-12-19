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
package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.BindingListener;

/**
 * ElectionCredentialsProvider that provides the server URI as credentials
 * for elections. Natural comparison of the URI string is used.
 */
public class ServerIdElectionCredentialsProvider
        implements ElectionCredentialsProvider, BindingListener
{
    private volatile URI me;

    @Override
    public void listeningAt( URI me )
    {
        this.me = me;
    }

    @Override
    public ElectionCredentials getCredentials( String role )
    {
        return new ServerIdElectionCredentials( me );
    }
}
