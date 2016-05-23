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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
//import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Stores role data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
// TODO: Extract shared code with FileUserRepository
public class FileRoleRepository extends LifecycleAdapter implements RoleRepository
{
    private final Path roleFile;

    /** Quick lookup of roles by name */
    private final Map<String,RoleRecord> rolesByName = new ConcurrentHashMap<>();
    private final Map<String, SortedSet<String>> rolesByUsername = new ConcurrentHashMap<>();

    private final Log log;

    /** Master list of roles */
    private volatile List<RoleRecord> roles = new ArrayList<>();

    private final RoleSerialization serialization = new RoleSerialization();

    public FileRoleRepository( Path file, LogProvider logProvider )
    {
        this.roleFile = file.toAbsolutePath();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RoleRecord findByName( String name )
    {
        return rolesByName.get( name );
    }

    @Override
    public Set<String> findByUsername( String username )
    {
        return rolesByUsername.get( username );
    }

    @Override
    public void start() throws Throwable
    {
        if ( Files.exists( roleFile ) )
        {
            loadRolesFromFile();
        }
    }

    @Override
    public void create( RoleRecord role ) throws IllegalCredentialsException, IOException
    {
        if ( !isValidName( role.name() ) )
        {
            throw new IllegalCredentialsException( "'" + role.name() + "' is not a valid role name." );
        }

        synchronized (this)
        {
            // Check for existing role
            for ( RoleRecord other : roles )
            {
                if ( other.name().equals( role.name() ) )
                {
                    throw new IllegalCredentialsException( "The specified role already exists" );
                }
            }

            roles.add( role );

            saveRolesToFile();

            rolesByName.put( role.name(), role );

            populateUserMap( role );
        }
    }

    @Override
    public void update( RoleRecord existingRole, RoleRecord updatedRole ) throws ConcurrentModificationException, IOException
    {
        // Assert input is ok
        if ( !existingRole.name().equals( updatedRole.name() ) )
        {
            throw new IllegalArgumentException( "updatedRole has a different name" );
        }

        synchronized (this)
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
                } else
                {
                    newRoles.add( other );
                }
            }

            if ( !foundRole )
            {
                throw new ConcurrentModificationException();
            }

            roles = newRoles;

            saveRolesToFile();

            rolesByName.put( updatedRole.name(), updatedRole );

            removeFromUserMap( existingRole );
            populateUserMap( updatedRole );
        }
    }

    @Override
    public boolean delete( RoleRecord role ) throws IOException
    {
        boolean foundRole = false;
        synchronized (this)
        {
            // Copy-on-write for the roles list
            List<RoleRecord> newRoles = new ArrayList<>();
            for ( RoleRecord other : roles )
            {
                if ( other.name().equals( role.name() ) )
                {
                    foundRole = true;
                } else
                {
                    newRoles.add( other );
                }
            }

            if ( foundRole )
            {
                roles = newRoles;

                saveRolesToFile();

                rolesByName.remove( role.name() );
            }

            removeFromUserMap( role );
        }
        return foundRole;
    }

    @Override
    public int numberOfRoles()
    {
        return roles.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        return name.matches( "^[a-zA-Z0-9_]+$" );
    }

    private void saveRolesToFile() throws IOException
    {
        Path directory = roleFile.getParent();
        if ( !Files.exists( directory ) )
        {
            Files.createDirectories( directory );
        }

        Path tempFile = Files.createTempFile( directory, roleFile.getFileName().toString() + "-", ".tmp" );
        try
        {
            Files.write( tempFile, serialization.serialize( roles ) );
            Files.move( tempFile, roleFile, ATOMIC_MOVE, REPLACE_EXISTING );
        } catch ( Throwable e )
        {
            Files.delete( tempFile );
            throw e;
        }
    }

    private void loadRolesFromFile() throws IOException
    {
        byte[] fileBytes = Files.readAllBytes( roleFile );
        List<RoleRecord> loadedRoles;
        try
        {
            loadedRoles = serialization.deserializeRoles( fileBytes );
        } catch ( RoleSerialization.FormatException e )
        {
            log.error( "Ignoring role file \"%s\" (%s)", roleFile.toAbsolutePath(), e.getMessage() );
            throw new IllegalStateException( "Failed to read role file: " + roleFile );
        }

        roles = loadedRoles;
        for ( RoleRecord role : roles )
        {
            rolesByName.put( role.name(), role );

            populateUserMap( role );
        }
    }

    private void populateUserMap( RoleRecord role )
    {
        for ( String username : role.users() )
        {
            SortedSet<String> memberOfRoles = rolesByUsername.get( username );
            if ( memberOfRoles == null )
            {
                memberOfRoles = new ConcurrentSkipListSet<>();
                rolesByUsername.put( username, memberOfRoles );
            }
            memberOfRoles.add( role.name() );
        }
    }

    private void removeFromUserMap( RoleRecord role )
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
