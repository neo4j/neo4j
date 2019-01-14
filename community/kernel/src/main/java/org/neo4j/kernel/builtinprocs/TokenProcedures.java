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
package org.neo4j.kernel.builtinprocs;

import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.WRITE;

public class TokenProcedures
{
    @Context
    public KernelTransaction tx;

    @Description( "Create a label" )
    @Procedure( name = "db.createLabel", mode = WRITE )
    public void createLabel( @Name( "newLabel" ) String newLabel )
            throws IllegalTokenNameException, TooManyLabelsException
    {

        tx.tokenWrite().labelGetOrCreateForName( newLabel );
    }

    @Description( "Create a RelationshipType" )
    @Procedure( name = "db.createRelationshipType", mode = WRITE )
    public void createRelationshipType( @Name( "newRelationshipType" ) String newRelationshipType ) throws IllegalTokenNameException
    {
        tx.tokenWrite().relationshipTypeGetOrCreateForName( newRelationshipType );
    }

    @Description( "Create a Property" )
    @Procedure( name = "db.createProperty", mode = WRITE )
    public void createProperty( @Name( "newProperty" ) String newProperty ) throws IllegalTokenNameException
    {
        tx.tokenWrite().propertyKeyGetOrCreateForName( newProperty );
    }

}
