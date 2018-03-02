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
package org.neo4j.server.security.enterprise.auth;

import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.server.security.auth.FileRepositorySerializer;
import org.neo4j.server.security.auth.exception.FormatException;

import static java.lang.String.format;

/**
 * Serializes role authorization and authentication data to a format similar to unix passwd files.
 */
public class RoleSerialization extends FileRepositorySerializer<RoleRecord>
{
    private static final String roleSeparator = ":";
    private static final String userSeparator = ",";

    @Override
    protected String serialize( RoleRecord role )
    {
        return String.join( roleSeparator, role.name(), String.join( userSeparator, role.users() ) );
    }

    @Override
    protected RoleRecord deserializeRecord( String line, int lineNumber ) throws FormatException
    {
        String[] parts = line.split( roleSeparator, -1 );
        if ( parts.length != 2 )
        {
            throw new FormatException( format( "wrong number of line fields [line %d]", lineNumber ) );
        }
        return new RoleRecord.Builder()
                .withName( parts[0] )
                .withUsers( deserializeUsers( parts[1] ) )
                .build();
    }

    private SortedSet<String> deserializeUsers( String part )
    {
        String[] splits = part.split( userSeparator, -1 );

        SortedSet<String> users = new TreeSet<>();

        for ( String user : splits )
        {
            if ( !user.trim().isEmpty() )
            {
                users.add( user );
            }
        }

        return users;
    }
}
