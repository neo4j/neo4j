/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cluster.client;

import org.apache.commons.lang3.RandomStringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class Cluster
{
    private final String name;
    private final List<Member> members = new ArrayList<>();

    /**
     * Creates a cluster with a random name.
     * The name is used in deciding which directory the store files are placed in.
     */
    public Cluster()
    {
        this( RandomStringUtils.randomAlphanumeric( 8 ) );
    }

    /**
     * Creates a cluster with a specified name.
     * The name is used in deciding which directory the store files are placed in.
     */
    public Cluster( @Nonnull String name )
    {
        requireNonNull( name );
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public List<Member> getMembers()
    {
        return members;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Cluster cluster = (Cluster) o;

        return name.equals( cluster.name ) && members.equals( cluster.members );

    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + members.hashCode();
        return result;
    }

    public boolean contains( URI serverId )
    {
        for ( Member member : members )
        {
            if ( serverId.toString().contains( member.getHost() ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Represents a member tag in the discovery XML file.
     */
    public static class Member
    {
        private String host;
        private boolean fullHaMember;

        public Member( int port, boolean fullHaMember )
        {
            this( localhost() + ":" + port, fullHaMember );
        }

        public Member( String host )
        {
            this( host, true );
        }

        public Member( String host, boolean fullHaMember )
        {
            this.host = host;
            this.fullHaMember = fullHaMember;
        }

        public boolean isFullHaMember()
        {
            return fullHaMember;
        }

        private static String localhost()
        {
            return "localhost";
        }

        public String getHost()
        {
            return host;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Member member = (Member) o;

            return host.equals( member.host );

        }

        @Override
        public int hashCode()
        {
            return host.hashCode();
        }
    }
}
