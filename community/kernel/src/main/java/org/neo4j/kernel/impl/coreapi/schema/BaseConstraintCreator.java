/*
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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;

public class BaseConstraintCreator implements ConstraintCreator
{
    protected final InternalSchemaActions actions;
    protected final Label label;

    public BaseConstraintCreator( InternalSchemaActions actions, Label label )
    {
        this.actions = actions;
        this.label = label;

        assertInUnterminatedTransaction();
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique( String propertyKey )
    {
        return new PropertyUniqueConstraintCreator( actions, label, propertyKey );
    }

    @Override
    public ConstraintDefinition create()
    {
        assertInUnterminatedTransaction();
        throw new IllegalStateException( "No constraint assertions specified" );
    }

    protected final void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
