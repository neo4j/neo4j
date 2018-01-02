/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.input.DataException;

import static java.lang.String.format;

public class DuplicateInputIdException extends DataException
{
    public DuplicateInputIdException( Object id, String groupName, String sourceLocation1, String sourceLocation2 )
    {
        super( message( id, groupName, sourceLocation1, sourceLocation2 ) );
    }

    public static String message( Object id, String groupName, String sourceLocation1, String sourceLocation2 )
    {
        return format( "Id '%s' is defined more than once in %s, at least at %s and %s",
                id, groupName, sourceLocation1, sourceLocation2 );
    }
}
