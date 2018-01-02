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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Cluster configuration. Includes name of cluster, list of nodes, and role mappings
 */
public class ClusterConfiguration
{
    public static final String COORDINATOR = "coordinator";

    private final String name;
    private final Log log;
    private final List<URI> candidateMembers;
    private volatile Map<InstanceId, URI> members;
    private volatile Map<String, InstanceId> roles = new HashMap<>();

    public ClusterConfiguration( String name, LogProvider logProvider, String... members )
    {
        this.name = name;
        this.log = logProvider.getLog( getClass() );
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
        this.members = new HashMap<InstanceId, URI>();
    }

    public ClusterConfiguration( String name, LogProvider logProvider, Collection<URI> members )
    {
        this.name = name;
        this.log = logProvider.getLog( getClass() );
        this.candidateMembers = new ArrayList<URI>( members );
        this.members = new HashMap<InstanceId, URI>();
    }

    public ClusterConfiguration( ClusterConfiguration copy )
    {
        this(copy, copy.log);
    }

    private ClusterConfiguration( ClusterConfiguration copy, Log log )
    {
        this.name = copy.name;
        this.log = log;
        this.candidateMembers = new ArrayList<URI>( copy.candidateMembers );
        this.roles = new HashMap<String, InstanceId>( copy.roles );
        this.members = new HashMap<InstanceId, URI>( copy.members );
    }

    public void joined( InstanceId joinedInstanceId, URI instanceUri )
    {
        if ( instanceUri.equals( members.get( joinedInstanceId ) ) )
        {
            return; // Already know that this node is in - ignore
        }

        Map<InstanceId,URI> newMembers = new HashMap<>( members );
        newMembers.put( joinedInstanceId, instanceUri );
        members = newMembers;
    }

    public void left( InstanceId leftInstanceId )
    {
        log.info( "Instance " + leftInstanceId + " is leaving the cluster" );
        Map<InstanceId,URI> newMembers = new HashMap<>( members );
        newMembers.remove( leftInstanceId );
        members = newMembers;

        // Remove any roles that this node had
        Iterator<Map.Entry<String, InstanceId>> entries = roles.entrySet().iterator();
        while ( entries.hasNext() )
        {
            Map.Entry<String, InstanceId> roleEntry = entries.next();

            if ( roleEntry.getValue().equals( leftInstanceId ) )
            {
                log.info("Removed role " + roleEntry.getValue() + " from leaving instance " + roleEntry.getKey() );
                entries.remove();
            }
        }
    }

    public void elected( String name, InstanceId electedInstanceId )
    {
        assert members.containsKey( electedInstanceId );
        Map<String,InstanceId> newRoles = new HashMap<>( roles );
        newRoles.put( name, electedInstanceId );
        roles = newRoles;
    }

    public void unelected( String roleName )
    {
        assert roles.containsKey( roleName );
        Map<String,InstanceId> newRoles = new HashMap<>( roles );
        newRoles.remove( roleName );
        roles = newRoles;
    }

    public void setMembers( Map<InstanceId, URI> members )
    {
        this.members = new HashMap<InstanceId, URI>( members );
    }

    public void setRoles( Map<String, InstanceId> roles )
    {
        for ( InstanceId electedInstanceId : roles.values() )
        {
            assert members.containsKey( electedInstanceId );
        }

        this.roles = new HashMap<String, InstanceId>( roles );
    }

    public Iterable<InstanceId> getMemberIds()
    {
        return members.keySet();
    }

    public Map<InstanceId, URI> getMembers()
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

    public Map<String, InstanceId> getRoles()
    {
        return roles;
    }

    public void left()
    {
        this.members = new HashMap<InstanceId, URI>();
        roles = new HashMap<String, InstanceId>();
    }

    public void removeElected( String roleName )
    {
        Map<String,InstanceId> newRoles = new HashMap<>( roles );
        InstanceId removed = newRoles.remove( roleName );
        roles = newRoles;
        log.info( "Removed role " + roleName + " from instance " + removed );
    }

    public InstanceId getElected( String roleName )
    {
        return roles.get( roleName );
    }

    public Iterable<String> getRolesOf( final InstanceId node )
    {
        return Iterables.map( new Function<Map.Entry<String, InstanceId>, String>()
        {
            @Override
            public String apply( Map.Entry<String, InstanceId> stringURIEntry )
            {
                return stringURIEntry.getKey();
            }
        }, Iterables.filter( new Predicate<Map.Entry<String, InstanceId>>()
        {
            @Override
            public boolean test( Map.Entry<String, InstanceId> item )
            {
                return item.getValue().equals( node );
            }
        }, roles.entrySet() ) );
    }

    public URI getUriForId( InstanceId node )
    {
        return members.get( node );
    }

    public InstanceId getIdForUri( URI fromUri )
    {
        for ( Map.Entry<InstanceId, URI> serverIdURIEntry : members.entrySet() )
        {
            if ( serverIdURIEntry.getValue().equals( fromUri ) )
            {
                return serverIdURIEntry.getKey();
            }
        }
        return null;
    }

    public ClusterConfiguration snapshot( Log log )
    {
        return new ClusterConfiguration( this, log );
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
        return result;
    }
}
