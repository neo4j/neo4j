/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.exceptions;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.exceptions.Status;

public class IndexHintException extends Neo4jException
{

    public IndexHintException( String label, List<String> properties, String message )
    {
        super( msg(label, properties, message) );
    }

    @Override
    public Status status()
    {
        return Status.Schema.IndexNotFound;
    }

    private static String msg( String label, List<String> properties, String message )
    {
        return String.format( "%s\nLabel: `%s`\nProperty name: %s}",
                              message,
                              label,
        properties.stream().map( p -> "'" + p + "'").collect( Collectors.joining( ",") ) );
    }
}
