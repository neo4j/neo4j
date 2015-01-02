/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.util.Map;

public class InconsistentlyUpgradedClusterException extends StoreFailureException
{
    private static final String MESSAGE_FORMAT = "Detected separately upgraded members of the cluster. " +
                                                 "This machine has %s, other members: %s";

    public InconsistentlyUpgradedClusterException( StoreId myStoreId, Map<Integer, StoreId> mismatched )
    {
        super( String.format( MESSAGE_FORMAT, myStoreId, formMismatchedString( mismatched ) ) );
    }

    private static String formMismatchedString( Map<Integer, StoreId> mismatched )
    {
        StringBuilder sb = new StringBuilder( "[" );
        for ( Map.Entry<Integer, StoreId> entry : mismatched.entrySet() )
        {
            sb.append( "Instance " ).append( entry.getKey() )
              .append( " has " ).append( entry.getValue() ).append( "," );
        }
        sb.setCharAt( sb.length() - 1, ']' );
        return sb.toString();
    }
}
