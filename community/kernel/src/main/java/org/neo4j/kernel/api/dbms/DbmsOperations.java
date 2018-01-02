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
package org.neo4j.kernel.api.dbms;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * Defines all types of system-oriented operations - i.e. those which do not read from or
 * write to the graph - that can be done from the {@link InwardKernel}.
 * An example of this is changing a user's password
 */
public interface DbmsOperations
{
    //=================================================
    //== PROCEDURE OPERATIONS ==
    //=================================================

    /** Invoke a DBMS procedure by name */
    RawIterator<Object[],ProcedureException> procedureCallDbms(
            QualifiedName name,
            Object[] input,
            SecurityContext securityContext
    ) throws ProcedureException;

    /** Invoke a DBMS function by name */
    Object functionCallDbms(
            QualifiedName name,
            Object[] input,
            SecurityContext securityContext
    ) throws ProcedureException;
}
