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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.api.exceptions.KernelException;

public class NodePropertyUniqueConstraintCreator extends BaseNodeConstraintCreator
{
    // Only single property key supported a.t.m.
    protected final String propertyKey;

    NodePropertyUniqueConstraintCreator( InternalSchemaActions internalCreator, Label label, String propertyKey )
    {
        super( internalCreator, label );
        this.propertyKey = propertyKey;
    }

    @Override
    public final NodePropertyUniqueConstraintCreator assertPropertyIsUnique( String propertyKey )
    {
        throw new UnsupportedOperationException( "You can only create one unique constraint at a time." );
    }

    @Override
    public final ConstraintDefinition create()
    {
        assertInUnterminatedTransaction();

        try
        {
            return actions.createPropertyUniquenessConstraint( label, propertyKey );
        }
        catch ( KernelException e )
        {
            String userMessage = actions.getUserMessage( e );
            throw new ConstraintViolationException( userMessage, e );
        }
    }
}
