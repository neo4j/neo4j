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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.string.UTF8;

import static java.lang.String.format;

/**
 * Serializes group authorization and authentication data to a format similar to unix passwd files.
 */
public class GroupSerialization
{
    public class FormatException extends Exception
    {
        FormatException( String message )
        {
            super( message );
        }
    }

    private static final String groupSeparator = ":";
    private static final String userSeparator = ",";

    public byte[] serialize(Collection<GroupRecord> groups)
    {
        StringBuilder sb = new StringBuilder();
        for ( GroupRecord group : groups )
        {
            sb.append( serialize(group) ).append( "\n" );
        }
        return UTF8.encode( sb.toString() );
    }

    public List<GroupRecord> deserializeGroups( byte[] bytes ) throws FormatException
    {
        List<GroupRecord> out = new ArrayList<>();
        int lineNumber = 1;
        for ( String line : UTF8.decode( bytes ).split( "\n" ) )
        {
            if (line.trim().length() > 0)
            {
                out.add( deserializeGroup( line, lineNumber ) );
            }
            lineNumber++;
        }
        return out;
    }

    private String serialize( GroupRecord group )
    {
        return join( groupSeparator, group.name(), serialize( group.users() ) );
    }

    private GroupRecord deserializeGroup( String line, int lineNumber ) throws FormatException
    {
        String[] parts = line.split( groupSeparator, -1 );
        if ( parts.length != 2 )
        {
            throw new FormatException( format( "wrong number of line fields [line %d]", lineNumber ) );
        }
        return new GroupRecord.Builder()
                .withName( parts[0] )
                .withUsers( deserializeUsers( parts[1], lineNumber ) )
                .build();
    }

    private String serialize( SortedSet<String> users )
    {
        return joinCollection( userSeparator, users );
    }

    private SortedSet<String> deserializeUsers( String part, int lineNumber ) throws FormatException
    {
        String[] splits = part.split( userSeparator, -1 );

        SortedSet<String> users = new TreeSet<>();

        for ( String user : splits )
        {
            users.add( user );
        }

        return users;
    }

    private String joinCollection( String separator, Collection<String> segments )
    {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for ( String segment : segments )
        {
            if ( i > 0 ) { sb.append( separator ); }
            sb.append( segment == null ? "" : segment );
            i++;
        }
        return sb.toString();
    }

    private String join( String separator, String... segments )
    {
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < segments.length; i++ )
        {
            if(i > 0) { sb.append( separator ); }
            sb.append( segments[i] == null ? "" : segments[i] );
        }
        return sb.toString();
    }
}
