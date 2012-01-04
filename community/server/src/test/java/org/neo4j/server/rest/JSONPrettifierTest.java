package org.neo4j.server.rest;

import static org.junit.Assert.assertFalse;

import org.junit.Ignore;
import org.junit.Test;

public class JSONPrettifierTest
{

    @Test
    @Ignore
    public void testEscapes()
    {
        String string = JSONPrettifier.parse( "{\"test\":\"{\\\"test\\\"}\"}" );
        assertFalse(string.contains( "\n}\"" ));
    }

}
