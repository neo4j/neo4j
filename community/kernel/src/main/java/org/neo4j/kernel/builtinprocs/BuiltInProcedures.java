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

import java.lang.management.ManagementFactory;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.proc.Procedures;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

/**
 * This registers procedures that are expected to be available by default in Neo4j.
 */
public class BuiltInProcedures implements ThrowingConsumer<Procedures, ProcedureException>
{
    private final String neo4jVersion;
    private final String neo4jEdition;

    public BuiltInProcedures( String neo4jVersion, String neo4jEdition )
    {
        this.neo4jVersion = neo4jVersion;
        this.neo4jEdition = neo4jEdition;
    }

    @Override
    public void accept( Procedures procs ) throws ProcedureException
    {
        // These are 'db'-namespaced procedures. They deal with database-level
        // functionality - eg. things that differ across databases
        procs.register( new ListLabelsProcedure( procedureName( "db", "labels" ) ) );
        procs.register( new ListPropertyKeysProcedure( procedureName( "db", "propertyKeys" ) ) );
        procs.register( new ListRelationshipTypesProcedure( procedureName( "db", "relationshipTypes" ) ) );
        procs.register( new ListIndexesProcedure( procedureName( "db", "indexes" ) ) );
        procs.register( new ListConstraintsProcedure( procedureName( "db", "constraints" ) ) );

        // These are 'sys'-namespaced procedures, they deal with DBMS-level
        // functionality - eg. things that apply across databases.
        procs.register( new ListProceduresProcedure( procedureName( "dbms", "procedures" ) ) );
        procs.register( new ListComponentsProcedure( procedureName( "dbms", "components" ), neo4jVersion, neo4jEdition ) );
        procs.register( new JmxQueryProcedure( procedureName( "dbms", "queryJmx" ), ManagementFactory.getPlatformMBeanServer() ) );

        // These are 'dbms'-namespaced procedures for dealing with authentication and authorization-oriented operations
        procs.register( new AlterUserPasswordProcedure( procedureName( "dbms", "changePassword" ) ) );
    }
}
