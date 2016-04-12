/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public final class CompiledReadOperationsUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledReadOperationsUtils()
    {
    }

    public static RelationshipIterator nodeGetRelationships(ReadOperations readOperations, long nodeId, Direction direction)
    {
        try
        {
            return readOperations.nodeGetRelationships( nodeId, direction );
        }
        catch ( EntityNotFoundException e )
        {
            throw new org.neo4j.cypher.internal.frontend.v3_0.CypherExecutionException(
                    e.getUserMessage( new org.neo4j.kernel.api.StatementTokenNameLookup( readOperations ) ), e );
        }
    }

}
