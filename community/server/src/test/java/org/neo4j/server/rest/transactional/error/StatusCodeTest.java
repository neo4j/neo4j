package org.neo4j.server.rest.transactional.error;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

import org.junit.Test;

public class StatusCodeTest
{
    @Test
    public void eachStatusCodeHasAUniqueNumber() throws Exception
    {
        // given
        HashSet<Integer> numbers = new HashSet<Integer>();

        // when
        for ( StatusCode statusCode : StatusCode.values() )
        {
            numbers.add( statusCode.getCode() );
        }

        // then
        assertEquals( StatusCode.values().length, numbers.size() );
    }
}
