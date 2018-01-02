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
package org.neo4j.cluster.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a clusters tag in the discovery XML file.
 */
public class Clusters
{
    private final List<Cluster> clusters = new ArrayList<Cluster>();
    private long timestamp;

    public void setTimestamp( long timestamp )
    {
        this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public List<Cluster> getClusters()
    {
        return clusters;
    }

    public Cluster getCluster( String name )
    {
        for ( Cluster cluster : clusters )
        {
            if ( cluster.getName().equals( name ) )
            {
                return cluster;
            }
        }
        return null;
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

        Clusters clusters1 = (Clusters) o;

        if ( !clusters.equals( clusters1.clusters ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return clusters.hashCode();
    }

    /**
     * Represents the cluster tag in the discovery XML file.
     */
    public static class Cluster
    {
        private final String name;
        private final List<Member> members = new ArrayList<Member>();

        public Cluster( String name )
        {
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

            if ( !name.equals( cluster.name ) )
            {
                return false;
            }
            if ( !members.equals( cluster.members ) )
            {
                return false;
            }

            return true;
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

        public Member getByUri( URI serverId )
        {
            for ( Member member : members )
            {
                if ( serverId.toString().contains( member.getHost() ) )
                {
                    return member;
                }
            }

            return null;
        }
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

            if ( !host.equals( member.host ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return host.hashCode();
        }
    }
}
