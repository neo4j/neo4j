/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.ClusterInstanceId;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Cluster configuration. Includes name of cluster, list of nodes, and role mappings
 */
public class ClusterConfiguration
{
    public static final String COORDINATOR = "coordinator";

    private final String name;
    private final StringLogger logger;
    private final List<URI> candidateMembers;
    private Map<ClusterInstanceId, URI> members;
    private Map<String, ClusterInstanceId> roles = new HashMap<String, ClusterInstanceId>();
    private int allowedFailures = 1;

    public ClusterConfiguration( String name, StringLogger logger, String... members )
    {
        this.name = name;
        this.logger = logger;
        this.candidateMembers = new ArrayList<URI>();
        for ( String node : members )
        {
            try
            {
                this.candidateMembers.add( new URI( node ) );
            }
            catch ( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
        this.members = new HashMap<ClusterInstanceId, URI>();
    }

    public ClusterConfiguration( String name, StringLogger logger, Collection<URI> members )
    {
        this.name = name;
        this.logger = logger;
        this.candidateMembers = new ArrayList<URI>( members );
        this.members = new HashMap<ClusterInstanceId, URI>();
    }

    public ClusterConfiguration( ClusterConfiguration copy )
    {
        this(copy, copy.logger);
    }

    private ClusterConfiguration( ClusterConfiguration copy, StringLogger logger )
    {
        this.name = copy.name;
        this.logger = logger;
        this.candidateMembers = new ArrayList<URI>( copy.candidateMembers );
        this.roles = new HashMap<String, ClusterInstanceId>( copy.roles );
        this.members = new HashMap<ClusterInstanceId, URI>( copy.members );
    }

    public void joined( ClusterInstanceId joinedInstanceId, URI instanceUri )
    {
        if ( instanceUri.equals( members.get( joinedInstanceId ) ) )
        {
            return; // Already know that this node is in - ignore
        }

        this.members = new HashMap<ClusterInstanceId, URI>( members );
        members.put( joinedInstanceId, instanceUri );
    }

    public void left( ClusterInstanceId leftInstanceId )
    {
        logger.info( "Instance " + leftInstanceId + " is leaving the cluster" );
        this.members = new HashMap<ClusterInstanceId, URI>( members );
        members.remove( leftInstanceId );

        // Remove any roles that this node had
        Iterator<Map.Entry<String, ClusterInstanceId>> entries = roles.entrySet().iterator();
        while ( entries.hasNext() )
        {
            Map.Entry<String, ClusterInstanceId> roleEntry = entries.next();

            if ( roleEntry.getValue().equals( leftInstanceId ) )
            {
                logger.info("Removed role " + roleEntry.getValue() + " from leaving instance " + roleEntry.getKey() );
                entries.remove();
            }
        }
    }

    public void elected( String name, ClusterInstanceId electedInstanceId )
    {
        assert members.containsKey( electedInstanceId );
        roles = new HashMap<String, ClusterInstanceId>( roles );
        roles.put( name, electedInstanceId );
    }

    public void unelected( String roleName )
    {
        assert roles.containsKey( roleName );
        roles = new HashMap<String, ClusterInstanceId>( roles );
        roles.remove( roleName );
    }

    public void setMembers( Map<ClusterInstanceId, URI> members )
    {
        this.members = new HashMap<ClusterInstanceId, URI>( members );
    }

    public void setRoles( Map<String, ClusterInstanceId> roles )
    {
        for ( ClusterInstanceId electedInstanceId : roles.values() )
        {
            assert members.containsKey( electedInstanceId );
        }

        this.roles = new HashMap<String, ClusterInstanceId>( roles );
    }

    public Iterable<ClusterInstanceId> getMemberIds()
    {
        return members.keySet();
    }

    public Map<ClusterInstanceId, URI> getMembers()
    {
        return members;
    }

    public List<URI> getMemberURIs()
    {
        return Iterables.toList( members.values() );
    }

    public String getName()
    {
        return name;
    }

    public Map<String, ClusterInstanceId> getRoles()
    {
        return roles;
    }

    public int getAllowedFailures()
    {
        return allowedFailures;
    }

    public void left()
    {
        this.members = new HashMap<ClusterInstanceId, URI>();
        roles = new HashMap<String, ClusterInstanceId>();
    }

    public void removeElected( String roleName )
    {
        roles = new HashMap<String, ClusterInstanceId>( roles );
        ClusterInstanceId removed = roles.remove( roleName );
        logger.info( "Removed role " + roleName + " from instance " + removed );
    }

    public ClusterInstanceId getElected( String roleName )
    {
        return roles.get( roleName );
    }

    public Iterable<String> getRolesOf( final ClusterInstanceId node )
    {
        return Iterables.map( new Function<Map.Entry<String, ClusterInstanceId>, String>()
        {
            @Override
            public String apply( Map.Entry<String, ClusterInstanceId> stringURIEntry )
            {
                return stringURIEntry.getKey();
            }
        }, Iterables.filter( new Predicate<Map.Entry<String, ClusterInstanceId>>()
        {
            @Override
            public boolean accept( Map.Entry<String, ClusterInstanceId> item )
            {
                return item.getValue().equals( node );
            }
        }, roles.entrySet() ) );
    }

    public URI getUriForId( ClusterInstanceId node )
    {
        return members.get( node );
    }

    public ClusterInstanceId getIdForUri( URI fromUri )
    {
        for ( Map.Entry<ClusterInstanceId, URI> serverIdURIEntry : members.entrySet() )
        {
            if ( serverIdURIEntry.getValue().equals( fromUri ) )
            {
                return serverIdURIEntry.getKey();
            }
        }
        return null;
    }

    public ClusterConfiguration snapshot(StringLogger logger)
    {
        return new ClusterConfiguration(this, logger);
    }

    @Override
    public String toString()
    {
        return "Name:" + name + " Nodes:" + members + " Roles:" + roles;
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

        ClusterConfiguration that = (ClusterConfiguration) o;

        if ( allowedFailures != that.allowedFailures )
        {
            return false;
        }
        if ( !candidateMembers.equals( that.candidateMembers ) )
        {
            return false;
        }
        if ( !members.equals( that.members ) )
        {
            return false;
        }
        if ( !name.equals( that.name ) )
        {
            return false;
        }
        if ( !roles.equals( that.roles ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + candidateMembers.hashCode();
        result = 31 * result + members.hashCode();
        result = 31 * result + roles.hashCode();
        result = 31 * result + allowedFailures;
        return result;
    }
}
