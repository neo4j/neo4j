/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

/**
 * Cluster configuration. Includes name of cluster, list of nodes, and role mappings
 */
public class ClusterConfiguration
{
    public static final String COORDINATOR = "coordinator";
    public static final String SLAVE = "slave";

    private final String name;
    private List<URI> members;
    private Map<String, URI> roles = new HashMap<String, URI>();
    private int allowedFailures = 1;

    public ClusterConfiguration( String name, String... members )
    {
        this.name = name;
        this.members = new ArrayList<URI>();
        for ( String node : members )
        {
            try
            {
                this.members.add( new URI( node ) );
            }
            catch ( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
    }

    public ClusterConfiguration( String name, Collection<URI> members )
    {
        this.name = name;
        this.members = new ArrayList<URI>( members );
    }

    public ClusterConfiguration( ClusterConfiguration copy )
    {
        this.name = copy.name;
        this.members = new ArrayList<URI>( copy.members );
        this.roles = new HashMap<String, URI>( copy.roles );
    }

    public void joined( URI nodeUrl )
    {
        if ( members.contains( nodeUrl ) )
        {
            return;
        }

        this.members = new ArrayList<URI>( members );
        members.add( nodeUrl );
    }

    public void left( URI nodeUrl )
    {
        this.members = new ArrayList<URI>( members );
        members.remove( nodeUrl );

        // Remove any roles that this node had
        Iterator<Map.Entry<String, URI>> entries = roles.entrySet().iterator();
        while ( entries.hasNext() )
        {
            Map.Entry<String, URI> roleEntry = entries.next();

            if ( roleEntry.getValue().equals( nodeUrl ) )
            {
                entries.remove();
            }
        }
    }

    public void elected( String name, URI node )
    {
        assert members.contains( node );
        roles = new HashMap<String, URI>( roles );
        roles.put( name, node );
    }

    public void setMembers( Iterable<URI> members )
    {
        this.members = new ArrayList<URI>();
        for ( URI node : members )
        {
            this.members.add( node );
        }
    }

    public void setRoles( Map<String, URI> roles )
    {
        for ( URI uri : roles.values() )
        {
            assert members.contains( uri );
        }

        this.roles.clear();
        this.roles.putAll( roles );
    }

    public List<URI> getMembers()
    {
        return members;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, URI> getRoles()
    {
        return roles;
    }

    public int getAllowedFailures()
    {
        return allowedFailures;
    }

    public void left()
    {
        this.members = new ArrayList<URI>();
        roles = new HashMap<String, URI>();
    }

    public void removeElected( String roleName )
    {
        roles = new HashMap<String, URI>( roles );
        roles.remove( roleName );
    }

    public URI getElected( String roleName )
    {
        return roles.get( roleName );
    }

    public Iterable<String> getRolesOf( final URI node )
    {
        return Iterables.map( new Function<Map.Entry<String, URI>, String>()
        {
            @Override
            public String apply( Map.Entry<String, URI> stringURIEntry )
            {
                return stringURIEntry.getKey();
            }
        }, Iterables.filter( new Predicate<Map.Entry<String, URI>>()
        {
            @Override
            public boolean accept( Map.Entry<String, URI> item )
            {
                return item.getValue().equals( node );
            }
        }, roles.entrySet() ) );
    }

    @Override
    public String toString()
    {
        return "Name:" + name + " Nodes:" + members + " Roles:" + roles;
    }
}
