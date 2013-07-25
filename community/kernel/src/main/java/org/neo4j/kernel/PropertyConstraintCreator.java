/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;

public class PropertyConstraintCreator extends BaseConstraintCreator
{
    // Only single property key supported a.t.m.
    protected final String propertyKey;

    PropertyConstraintCreator( InternalSchemaActions internalCreator, Label label, String propertyKeyOrNull )
    {
        super( internalCreator, label );
        this.propertyKey = propertyKeyOrNull;
    }

    @Override
    public final ConstraintCreator on( String propertyKey )
    {
        if ( this.propertyKey == null )
        {
            return doOn( propertyKey );
        }

        throw new UnsupportedOperationException(
            "Constraints on compound keys are not yet supported, only one property per constraint is allowed." );
    }

    @Override
    public ConstraintCreator unique()
    {
        return new PropertyUniqueConstraintCreator( actions, label, propertyKey );
    }

    @Override
    public final ConstraintDefinition create()
    {
        if ( propertyKey == null )
            super.create();
        return doCreate();
    }

    protected ConstraintCreator doOn( String propertyKey )
    {
        return new PropertyConstraintCreator( actions, label, propertyKey );
    }

    protected ConstraintDefinition doCreate()
    {
        throw new IllegalStateException( "Property key " + propertyKey + " specified, but not what it's for" );
    }
}
