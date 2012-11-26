package org.neo4j.helpers;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Strings.decamelize;

import org.junit.Test;

public class StringsTest
{
    @Test
    public void testDecamelize()
    {
        assertEquals( "foo", decamelize.apply( "foo" ) );
        assertEquals( "foo", decamelize.apply( "Foo" ) );
        assertEquals( "foo_bar", decamelize.apply( "FooBar" ) );
        assertEquals( "f_b", decamelize.apply( "FB" ) );
        assertEquals("_", decamelize.apply( "_" ) );
        // What is expected behaviour here?
//        assertEquals( "f_b", decamelize.apply( "F_B" ) );
    }
}
