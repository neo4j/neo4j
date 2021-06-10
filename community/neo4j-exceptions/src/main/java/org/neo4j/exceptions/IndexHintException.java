/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.common.EntityType;
import org.neo4j.kernel.api.exceptions.Status;

public class IndexHintException extends Neo4jException
{

    public IndexHintException( String labelOrRelType, List<String> properties, EntityType entityType )
    {
        super( msg( labelOrRelType, properties, entityType ) );
    }

    @Override
    public Status status()
    {
        return Status.Schema.IndexNotFound;
    }

    private static String msg( String labelOrRelType, List<String> properties, EntityType entityType )
    {
        String propertyNames = properties.stream().map( p -> ".`" + p + "`" ).collect( Collectors.joining( ", " ) );
        String indexFormatString;
        switch ( entityType )
        {
        case NODE:
            indexFormatString = String.format( "INDEX FOR (:`%s`) ON (%s)", labelOrRelType, propertyNames );
            break;
        case RELATIONSHIP:
            indexFormatString = String.format( "INDEX FOR ()-[:`%s`]-() ON (%s)", labelOrRelType, propertyNames );
            break;
        default:
            indexFormatString = "";
            break;
        }
        return String.format( "No such index: %s", indexFormatString );
    }
}
