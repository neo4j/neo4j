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
package org.neo4j.server.rest.transactional;

import org.neo4j.cypher.CouldNotCreateConstraintException;
import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.InternalException;
import org.neo4j.cypher.ParameterNotFoundException;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.helpers.Function;
import org.neo4j.server.rest.transactional.error.StatusCode;

public class CypherExceptionMapping implements Function<CypherException, StatusCode>
{
    @Override
    public StatusCode apply( CypherException e )
    {
        if ( ParameterNotFoundException.class.isInstance( e ) )
            return StatusCode.STATEMENT_MISSING_PARAMETER;

        if ( SyntaxException.class.isInstance( e ) )
            return StatusCode.STATEMENT_SYNTAX_ERROR;

        if ( CouldNotCreateConstraintException.class.isInstance( e ) )
            return StatusCode.COULD_NOT_CREATE_CONSTRAINT;

        if ( InternalException.class.isInstance( e ) )
            return StatusCode.INTERNAL_STATEMENT_EXECUTION_ERROR;

        return StatusCode.STATEMENT_EXECUTION_ERROR;
    }
}
