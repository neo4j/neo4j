package org.neo4j.server.rest.transactional;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.transactional.error.StatusCode.INTERNAL_STATEMENT_EXECUTION_ERROR;
import static org.neo4j.server.rest.transactional.error.StatusCode.STATEMENT_EXECUTION_ERROR;
import static org.neo4j.server.rest.transactional.error.StatusCode.STATEMENT_MISSING_PARAMETER_ERROR;
import static org.neo4j.server.rest.transactional.error.StatusCode.STATEMENT_SYNTAX_ERROR;

import org.junit.Test;
import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.InternalException;
import org.neo4j.cypher.ParameterNotFoundException;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.server.rest.transactional.error.StatusCode;

public class CypherExceptionMappingTest
{
    @Test
    public void shouldMap_SyntaxException_to_STATEMENT_SYNTAX_ERROR() throws Exception
    {
        assertEquals( STATEMENT_SYNTAX_ERROR, map( new SyntaxException( "message" ) ));
    }

    @Test
    public void shouldMap_ParameterNotFoundException_to_STATEMENT_MISSING_PARAMETER_EROR() throws Exception
    {
        assertEquals( STATEMENT_MISSING_PARAMETER_ERROR, map( new ParameterNotFoundException( "message" ) ));
    }

    @Test
    public void shouldMap_InternalException_to_UNKNOWN_STATEMENT_EXECUTION_ERROR() throws Exception
    {
        assertEquals( INTERNAL_STATEMENT_EXECUTION_ERROR, map( new InternalException( "message", null ) ));
    }

    @Test
    public void shouldMap_CypherException_to_STATEMENT_EXECUTION_ERROR() throws Exception
    {
        assertEquals( STATEMENT_EXECUTION_ERROR, map( new CypherException( "message", null ) {} ));
    }

    private StatusCode map( CypherException cypherException )
    {
        return new CypherExceptionMapping().apply( cypherException );
    }
}
