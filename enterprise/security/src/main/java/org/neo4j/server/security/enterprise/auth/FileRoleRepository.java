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
import org.neo4j.server.security.auth.exception.FormatException;

/**
 * Stores role data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileRoleRepository extends AbstractRoleRepository
{
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
            List<RoleRecord> loadedRoles;
            try
            {
                loadedRoles = serialization.loadRecordsFromFile( roleFile );
            }
            catch ( FormatException e )
            {
                log.error( "Failed to read role file \"%s\" (%s)", roleFile.toAbsolutePath(), e.getMessage() );
                throw new IllegalStateException( "Failed to read role file '" + roleFile + "'." );
            }

            roles = loadedRoles;
            for ( RoleRecord role : roles )
            {
                rolesByName.put( role.name(), role );

                populateUserMap( role );
            }
        }
    }

    @Override
    protected void saveRoles() throws IOException
    {
        serialization.saveRecordsToFile( roleFile, roles );
    }
}
