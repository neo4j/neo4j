package org.neo4j.server.rest.transactional;

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
            return StatusCode.STATEMENT_MISSING_PARAMETER_ERROR;

        if ( SyntaxException.class.isInstance( e ) )
            return StatusCode.STATEMENT_SYNTAX_ERROR;

        if ( InternalException.class.isInstance( e ) )
            return StatusCode.INTERNAL_STATEMENT_EXECUTION_ERROR;

        return StatusCode.STATEMENT_EXECUTION_ERROR;
    }
}
