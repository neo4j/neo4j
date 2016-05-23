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
package org.neo4j.server.security.enterprise.auth;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/** A role repository implementation that just stores roles in memory */
public class InMemoryRoleRepository extends LifecycleAdapter implements RoleRepository
{
    private final ConcurrentHashMap<String,RoleRecord> roles = new ConcurrentHashMap<>();
    private final Map<String, SortedSet<String>> rolesByUsername = new ConcurrentHashMap<>();

    @Override
    public RoleRecord findByName( String name )
    {
        return roles.get( name );
    }

    @Override
    public Set<String> findByUsername( String username )
    {
        return rolesByUsername.get( username );
    }

    @Override
    public void create( RoleRecord role ) throws IllegalCredentialsException
    {
        synchronized (this)
        {
            // Check for existing role or token
            for ( RoleRecord other : roles.values() )
            {
                if ( other.name().equals( role.name() ) )
                {
                    throw new IllegalCredentialsException( "The specified role already exists" );
                }
            }

            roles.put( role.name(), role );

            populateUserMap( role );
        }
    }

    @Override
    public void update( RoleRecord existingRole, RoleRecord updatedRole ) throws ConcurrentModificationException
    {
        // Assert input is ok
        if ( !existingRole.name().equals( updatedRole.name() ) )
        {
            throw new IllegalArgumentException( "updated role has a different name" );
        }

        synchronized (this)
        {
            boolean foundRole = false;
            for ( RoleRecord other : roles.values() )
            {
                if ( other.equals( existingRole ) )
                {
                    foundRole = true;
                }
            }

            if ( !foundRole )
            {
                throw new ConcurrentModificationException();
            }

            roles.put( updatedRole.name(), updatedRole );

            removeFromUserMap( existingRole );
            populateUserMap( updatedRole );
        }
    }

    @Override
    public boolean delete( RoleRecord role )
    {
        synchronized (this)
        {
            boolean removedRole = roles.remove( role.name() ) != null;

            if ( removedRole )
            {
                removeFromUserMap( role );
            }

            return removedRole;
        }
    }

    @Override
    public int numberOfRoles()
    {
        return roles.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        // This repo can store any name
        return true;
    }

    private void populateUserMap( RoleRecord role )
    {
        for ( String username : role.users() )
        {
            SortedSet<String> membersOfRole = rolesByUsername.get( username );
            if ( membersOfRole == null )
            {
                membersOfRole = new ConcurrentSkipListSet<>();
                rolesByUsername.put( username, membersOfRole );
            }
            membersOfRole.add( role.name() );
        }
    }

    private void removeFromUserMap( RoleRecord role )
    {
        for ( String username : role.users() )
        {
            SortedSet<String> membersOfRole = rolesByUsername.get( username );
            if ( membersOfRole != null )
            {
                membersOfRole.remove( role.name() );
            }
        }
    }

}
