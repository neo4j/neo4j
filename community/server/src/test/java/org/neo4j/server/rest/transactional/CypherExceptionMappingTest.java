/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import org.junit.Test;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.InternalException;
import org.neo4j.cypher.ParameterNotFoundException;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.server.rest.transactional.error.Status;

import static org.junit.Assert.assertEquals;

import static org.neo4j.server.rest.transactional.error.Status.Statement.ExecutionFailure;
import static org.neo4j.server.rest.transactional.error.Status.Statement.InvalidSyntax;
import static org.neo4j.server.rest.transactional.error.Status.Statement.ParameterMissing;

public class CypherExceptionMappingTest
{
    @Test
    public void shouldMap_SyntaxException_to_STATEMENT_SYNTAX_ERROR() throws Exception
    {
        assertEquals( InvalidSyntax, map( new SyntaxException( "message" ) ));
    }

    @Test
    public void shouldMap_ParameterNotFoundException_to_STATEMENT_MISSING_PARAMETER() throws Exception
    {
        assertEquals( ParameterMissing, map( new ParameterNotFoundException( "message" ) ));
    }

    @Test
    public void shouldMap_InternalException_to_INTERNAL_STATEMENT_EXECUTION_ERROR() throws Exception
    {
        assertEquals( ExecutionFailure, map( new InternalException( "message", null ) ));
    }

    @Test
    public void shouldMap_CypherException_to_STATEMENT_EXECUTION_ERROR() throws Exception
    {
        assertEquals( ExecutionFailure, map( new CypherException( "message", null ) {} ));
    }

    private Status map( CypherException cypherException )
    {
        return new CypherExceptionMapping().apply( cypherException );
    }
}
