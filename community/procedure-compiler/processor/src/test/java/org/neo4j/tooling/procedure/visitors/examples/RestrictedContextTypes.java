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
package org.neo4j.tooling.procedure.visitors.examples;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.ProcedureTransaction;
import org.neo4j.procedure.TerminationGuard;

public class RestrictedContextTypes
{

    // BELOW ARE TYPES ALLOWED FOR ANY PROCEDURE|FUNCTION

    @Context
    public GraphDatabaseService graphDatabaseService;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureTransaction procedureTransaction;

    // BELOW ARE RESTRICTED TYPES, THESE ARE UNSUPPORTED AND SUBJECT TO CHANGE

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public KernelTransaction kernelTransaction;

    @Context
    public DependencyResolver dependencyResolver;

    @Context
    public UserManager userManager;
}
