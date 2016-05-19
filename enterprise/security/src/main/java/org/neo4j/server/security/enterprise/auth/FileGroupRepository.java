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
 * Stores group data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
// TODO: Extract shared code with FileUserRepository
public class FileGroupRepository extends LifecycleAdapter implements GroupRepository
{
    private final Path groupFile;

    /** Quick lookup of groups by name */
    private final Map<String,GroupRecord> groupsByName = new ConcurrentHashMap<>();
    private final Map<String, SortedSet<String>> groupsByUsername = new ConcurrentHashMap<>();

    private final Log log;

    /** Master list of groups */
    private volatile List<GroupRecord> groups = new ArrayList<>();

    private final GroupSerialization serialization = new GroupSerialization();

    public FileGroupRepository( Path file, LogProvider logProvider )
    {
        this.groupFile = file.toAbsolutePath();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public GroupRecord findByName( String name )
    {
        return groupsByName.get( name );
    }

    @Override
    public Set<String> findByUsername( String username )
    {
        return groupsByUsername.get( username );
    }

    @Override
    public void start() throws Throwable
    {
        if ( Files.exists( groupFile ) )
        {
            loadGroupsFromFile();
        }
    }

    @Override
    public void create( GroupRecord group ) throws IllegalCredentialsException, IOException
    {
        if ( !isValidName( group.name() ) )
        {
            throw new IllegalCredentialsException( "'" + group.name() + "' is not a valid group name." );
        }

        synchronized (this)
        {
            // Check for existing group
            for ( GroupRecord other : groups )
            {
                if ( other.name().equals( group.name() ) )
                {
                    throw new IllegalCredentialsException( "The specified group already exists" );
                }
            }

            groups.add( group );

            saveGroupsToFile();

            groupsByName.put( group.name(), group );

            populateUserMap( group );
        }
    }

    @Override
    public void update( GroupRecord existingGroup, GroupRecord updatedGroup ) throws ConcurrentModificationException, IOException
    {
        // Assert input is ok
        if ( !existingGroup.name().equals( updatedGroup.name() ) )
        {
            throw new IllegalArgumentException( "updatedGroup has a different name" );
        }

        synchronized (this)
        {
            // Copy-on-write for the groups list
            List<GroupRecord> newGroups = new ArrayList<>();
            boolean foundGroup = false;
            for ( GroupRecord other : groups )
            {
                if ( other.equals( existingGroup ) )
                {
                    foundGroup = true;
                    newGroups.add( updatedGroup );
                } else
                {
                    newGroups.add( other );
                }
            }

            if ( !foundGroup )
            {
                throw new ConcurrentModificationException();
            }

            groups = newGroups;

            saveGroupsToFile();

            groupsByName.put( updatedGroup.name(), updatedGroup );

            removeFromUserMap( existingGroup );
            populateUserMap( updatedGroup );
        }
    }

    @Override
    public boolean delete( GroupRecord group ) throws IOException
    {
        boolean foundGroup = false;
        synchronized (this)
        {
            // Copy-on-write for the groups list
            List<GroupRecord> newGroups = new ArrayList<>();
            for ( GroupRecord other : groups )
            {
                if ( other.name().equals( group.name() ) )
                {
                    foundGroup = true;
                } else
                {
                    newGroups.add( other );
                }
            }

            if ( foundGroup )
            {
                groups = newGroups;

                saveGroupsToFile();

                groupsByName.remove( group.name() );
            }

            removeFromUserMap( group );
        }
        return foundGroup;
    }

    @Override
    public int numberOfGroups()
    {
        return groups.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        return name.matches( "^[a-zA-Z0-9_]+$" );
    }

    private void saveGroupsToFile() throws IOException
    {
        Path directory = groupFile.getParent();
        if ( !Files.exists( directory ) )
        {
            Files.createDirectories( directory );
        }

        Path tempFile = Files.createTempFile( directory, groupFile.getFileName().toString() + "-", ".tmp" );
        try
        {
            Files.write( tempFile, serialization.serialize( groups ) );
            Files.move( tempFile, groupFile, ATOMIC_MOVE, REPLACE_EXISTING );
        } catch ( Throwable e )
        {
            Files.delete( tempFile );
            throw e;
        }
    }

    private void loadGroupsFromFile() throws IOException
    {
        byte[] fileBytes = Files.readAllBytes( groupFile );
        List<GroupRecord> loadedGroups;
        try
        {
            loadedGroups = serialization.deserializeGroups( fileBytes );
        } catch ( GroupSerialization.FormatException e )
        {
            log.error( "Ignoring group file \"%s\" (%s)", groupFile.toAbsolutePath(), e.getMessage() );
            throw new IllegalStateException( "Failed to read group file: " + groupFile );
        }

        groups = loadedGroups;
        for ( GroupRecord group : groups )
        {
            groupsByName.put( group.name(), group );

            populateUserMap( group );
        }
    }

    private void populateUserMap( GroupRecord group )
    {
        for ( String username : group.users() )
        {
            SortedSet<String> memberOfGroups = groupsByUsername.get( username );
            if ( memberOfGroups == null )
            {
                memberOfGroups = new ConcurrentSkipListSet<>();
                groupsByUsername.put( username, memberOfGroups );
            }
            memberOfGroups.add( group.name() );
        }
    }

    private void removeFromUserMap( GroupRecord group )
    {
        for ( String username : group.users() )
        {
            SortedSet<String> memberOfGroups = groupsByUsername.get( username );
            if ( memberOfGroups != null )
            {
                memberOfGroups.remove( group.name() );
            }
        }
    }
}
