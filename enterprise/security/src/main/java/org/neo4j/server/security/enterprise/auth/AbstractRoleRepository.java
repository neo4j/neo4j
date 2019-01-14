/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

import static org.neo4j.helpers.collection.MapUtil.trimToFlattenedList;
import static org.neo4j.helpers.collection.MapUtil.trimToList;

public abstract class AbstractRoleRepository extends LifecycleAdapter implements RoleRepository
{
    // TODO: We could improve concurrency by using a ReadWriteLock

    /** Quick lookup of roles by name */
    private final Map<String,RoleRecord> rolesByName = new ConcurrentHashMap<>();
    private final Map<String,SortedSet<String>> rolesByUsername = new ConcurrentHashMap<>();

    /** Master list of roles */
    protected volatile List<RoleRecord> roles = new ArrayList<>();
    protected AtomicLong lastLoaded = new AtomicLong( 0L );

    private final Pattern roleNamePattern = Pattern.compile( "^[a-zA-Z0-9_]+$" );

    @Override
    public void clear()
    {
        roles.clear();
        rolesByName.clear();
        rolesByUsername.clear();
    }

    @Override
    public RoleRecord getRoleByName( String roleName )
    {
        return roleName == null ? null : rolesByName.get( roleName );
    }

    @Override
    public Set<String> getRoleNamesByUsername( String username )
    {
        Set<String> roleNames = rolesByUsername.get( username );
        return roleNames != null ? roleNames : Collections.emptySet();
    }

    @Override
    public void create( RoleRecord role ) throws InvalidArgumentsException, IOException
    {
        assertValidRoleName( role.name() );

        synchronized ( this )
        {
            // Check for existing role
            for ( RoleRecord other : roles )
            {
                if ( other.name().equals( role.name() ) )
                {
                    throw new InvalidArgumentsException( "The specified role '" + role.name() + "' already exists." );
                }
            }

            roles.add( role );

            persistRoles();

            rolesByName.put( role.name(), role );

            populateUserMap( role );
        }
    }

    @Override
    public void setRoles( ListSnapshot<RoleRecord> rolesSnapshot ) throws InvalidArgumentsException
    {
        for ( RoleRecord role : rolesSnapshot.values() )
        {
            assertValidRoleName( role.name() );
        }

        synchronized ( this )
        {
            roles.clear();

            this.roles.addAll( rolesSnapshot.values() );
            this.lastLoaded.set( rolesSnapshot.timestamp() );

            trimToList( rolesByName, roles, RoleRecord::name );
            trimToFlattenedList( rolesByUsername, roles, r -> r.users().stream() );

            for ( RoleRecord role : roles )
            {
                rolesByName.put( role.name(), role );
                populateUserMap( role );
            }
        }
    }

    @Override
    public void update( RoleRecord existingRole, RoleRecord updatedRole )
            throws ConcurrentModificationException, IOException
    {
        // Assert input is ok
        if ( !existingRole.name().equals( updatedRole.name() ) )
        {
            throw new IllegalArgumentException( "The attempt to update the role from '" + existingRole.name() +
                    "' to '" + updatedRole.name() + "' failed. Changing a roles name is not allowed." );
        }

        synchronized ( this )
        {
            // Copy-on-write for the roles list
            List<RoleRecord> newRoles = new ArrayList<>();
            boolean foundRole = false;
            for ( RoleRecord other : roles )
            {
                if ( other.equals( existingRole ) )
                {
                    foundRole = true;
                    newRoles.add( updatedRole );
                }
                else
                {
                    newRoles.add( other );
                }
            }

            if ( !foundRole )
            {
                throw new ConcurrentModificationException();
            }

            roles = newRoles;

            persistRoles();

            rolesByName.put( updatedRole.name(), updatedRole );

            removeFromUserMap( existingRole );
            populateUserMap( updatedRole );
        }
    }

    @Override
    public synchronized boolean delete( RoleRecord role ) throws IOException
    {
        boolean foundRole = false;
        // Copy-on-write for the roles list
        List<RoleRecord> newRoles = new ArrayList<>();
        for ( RoleRecord other : roles )
        {
            if ( other.name().equals( role.name() ) )
            {
                foundRole = true;
            }
            else
            {
                newRoles.add( other );
            }
        }

        if ( foundRole )
        {
            roles = newRoles;

            persistRoles();

            rolesByName.remove( role.name() );
        }

        removeFromUserMap( role );
        return foundRole;
    }

    @Override
    public synchronized int numberOfRoles()
    {
        return roles.size();
    }

    @Override
    public void assertValidRoleName( String name ) throws InvalidArgumentsException
    {
        if ( name == null || name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided role name is empty." );
        }
        if ( !roleNamePattern.matcher( name ).matches() )
        {
            throw new InvalidArgumentsException(
                    "Role name '" + name + "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    @Override
    public synchronized void removeUserFromAllRoles( String username )
            throws ConcurrentModificationException, IOException
    {
        Set<String> roles = rolesByUsername.get( username );
        if ( roles != null )
        {
            // Since update() is modifying the set we create a copy for the iteration
            List<String> rolesToRemoveFrom = new ArrayList<>( roles );
            for ( String roleName : rolesToRemoveFrom )
            {
                RoleRecord role = rolesByName.get( roleName );
                RoleRecord newRole = role.augment().withoutUser( username ).build();
                update( role, newRole );
            }
        }
    }

    @Override
    public synchronized Set<String> getAllRoleNames()
    {
        return roles.stream().map( RoleRecord::name ).collect( Collectors.toSet() );
    }

    /**
     * Override this in the implementing class to persist roles
     *
     * @throws IOException
     */
    protected abstract void persistRoles() throws IOException;

    /**
     * Override this in the implementing class to load roles
     *
     * @return a timestamped snapshot of roles, or null if the backing file did not exist
     * @throws IOException
     */
    protected abstract ListSnapshot<RoleRecord> readPersistedRoles() throws IOException;

    // ------------------ helpers --------------------

    protected void populateUserMap( RoleRecord role )
    {
        for ( String username : role.users() )
        {
            SortedSet<String> memberOfRoles = rolesByUsername.computeIfAbsent( username, k -> new ConcurrentSkipListSet<>() );
            memberOfRoles.add( role.name() );
        }
    }

    protected void removeFromUserMap( RoleRecord role )
    {
        for ( String username : role.users() )
        {
            SortedSet<String> memberOfRoles = rolesByUsername.get( username );
            if ( memberOfRoles != null )
            {
                memberOfRoles.remove( role.name() );
            }
        }
    }
}
