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
package org.neo4j.kernel.builtinprocs;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.proc.Procedures;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

/**
 * This registers procedures that are expected to be available by default in Neo4j.
 */
public class BuiltInProcedures
{
    public static void addTo( Procedures procs ) throws ProcedureException
    {
        procs.register( new ListLabelsProcedure( procedureName( "sys", "db", "labels" ) ) );
        procs.register( new ListPropertyKeysProcedure( procedureName( "sys", "db", "propertyKeys" ) ) );
        procs.register( new ListRelationshipTypesProcedure( procedureName( "sys", "db", "relationshipTypes" ) ) );
    }
}
