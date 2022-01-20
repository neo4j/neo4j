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

/**
 * A hint was used referring to an index that does not exist.
 */
public class IndexHintException extends Neo4jException
{
    public enum IndexHintIndexType
    {
        ANY,
        BTREE,
        TEXT,
        RANGE
    }

    public IndexHintException( String variableName,
                               String labelOrRelType,
                               List<String> properties,
                               EntityType entityType,
                               IndexHintIndexType indexType )
    {
        super( msg( variableName, labelOrRelType, properties, entityType, indexType ) );
    }

    @Override
    public Status status()
    {
        return Status.Schema.IndexNotFound;
    }

    private static String msg( String variableName,
                               String labelOrRelType,
                               List<String> properties,
                               EntityType entityType,
                               IndexHintIndexType indexType )
    {
        return String.format( "No such index: %s", indexFormatString( variableName, labelOrRelType, properties, entityType, indexType ) );
    }

    public static String indexFormatString( String variableName,
                                            String labelOrRelType,
                                            List<String> properties,
                                            EntityType entityType,
                                            IndexHintIndexType indexType )
    {
        String escapedVarName = escape( variableName );

        String escapedLabelOrRelTypeName = escape( labelOrRelType );

        String propertyNames = properties
                .stream()
                .map( propertyName -> escapedVarName + "." + escape( propertyName ) )
                .collect( Collectors.joining( ", " ) );

        String typeString;
        switch ( indexType )
        {
        case BTREE:
            typeString = "BTREE ";
            break;
        case TEXT:
            typeString = "TEXT ";
            break;
        case RANGE:
            typeString = "RANGE ";
            break;
        default:
            typeString = "";
            break;
        }

        String indexFormatString;
        switch ( entityType )
        {
        case NODE:
            indexFormatString = String.format( "%sINDEX FOR (%s:%s) ON (%s)", typeString, escapedVarName, escapedLabelOrRelTypeName, propertyNames );
            break;
        case RELATIONSHIP:
            indexFormatString = String.format( "%sINDEX FOR ()-[%s:%s]-() ON (%s)", typeString, escapedVarName, escapedLabelOrRelTypeName, propertyNames );
            break;
        default:
            indexFormatString = "";
            break;
        }
        return indexFormatString;
    }

    private static String escape( String str )
    {
        return "`" + str.replace( "`", "``" ) + "`";
    }
}
