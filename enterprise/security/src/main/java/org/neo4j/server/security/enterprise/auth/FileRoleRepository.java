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
import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Stores role data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileRoleRepository extends AbstractRoleRepository
{
    // TODO: Extract shared code with FileUserRepository

    private final Path roleFile;

    private final Log log;

    private final RoleSerialization serialization = new RoleSerialization();

    public FileRoleRepository( Path file, LogProvider logProvider )
    {
        this.roleFile = file.toAbsolutePath();
        this.log = logProvider.getLog( getClass() );
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
    public boolean isValidName( String name )
    {
        return name.matches( "^[a-zA-Z0-9_]+$" );
    }

    @Override
    protected void saveRoles() throws IOException
    {
        saveRolesToFile();
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
        }
        catch ( Throwable e )
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
        }
        catch ( RoleSerialization.FormatException e )
        {
            log.error( "Failed to read role file \"%s\" (%s)", roleFile.toAbsolutePath(), e.getMessage() );
            throw new IllegalStateException( "Failed to read role file: " + roleFile );
        }

        roles = loadedRoles;
        for ( RoleRecord role : roles )
        {
            rolesByName.put( role.name(), role );

            populateUserMap( role );
        }
    }
}
