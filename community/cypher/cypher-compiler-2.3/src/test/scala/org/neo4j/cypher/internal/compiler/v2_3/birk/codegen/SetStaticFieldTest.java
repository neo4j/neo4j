package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SetStaticFieldTest
{
    @Test
    public void shouldAssignFields()
    {
        // when
        setStaticField.apply( Apa.class, "X", "HELLO WORLD!" );

        // then
        assertEquals( Apa.X, "HELLO WORLD!" );
    }

    public static class Apa
    {
        public static String X;
    }
}