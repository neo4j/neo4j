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
package org.neo4j.kernel.api.index;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public class DuplicateIndexEntryConflictException extends IndexEntryConflictException
{
    private final Object propertyValue;
    private final Set<Long> conflictingNodeIds;

    public DuplicateIndexEntryConflictException( Object propertyValue, Set<Long> conflictingNodeIds )
    {
        super( String.format( "Multiple nodes have property value %s:%n" +
                "  %s", quote( propertyValue ), asNodeList( conflictingNodeIds ) ) );
        this.propertyValue = propertyValue;
        this.conflictingNodeIds = conflictingNodeIds;
    }

    public Object getPropertyValue()
    {
        return propertyValue;
    }

    @Override
    public String evidenceMessage( String labelName, String propertyKey )
    {
        return String.format( "Multiple nodes with label `%s` have property `%s` = %s:%n" +
                "  %s", labelName, propertyKey, quote( propertyValue ), asNodeList(conflictingNodeIds) );
    }

    private static String asNodeList( Collection<Long> nodeIds )
    {
        TreeSet<Long> ids = new TreeSet<Long>( nodeIds );
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for ( long nodeId : ids )
        {
            if ( !first )
            {
                builder.append( ", " );
            }
            else
            {
                first = false;
            }
            builder.append( "node(" ).append( nodeId ).append( ")" );
        }
        return builder.toString();
    }
}
