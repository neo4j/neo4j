package org.neo4j.commandline.arguments;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OptionalBooleanArgTest
{

    @Test
    public void parsesValues1() throws Exception
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", false, "" );

        assertEquals( "false", arg.parse() );
        assertEquals( "false", arg.parse("--foo=false") );
        assertEquals( "true", arg.parse("--foo=true") );
        assertEquals( "true", arg.parse("--foo") );
    }

    @Test
    public void parsesValues2() throws Exception
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", true, "" );

        assertEquals( "true", arg.parse() );
        assertEquals( "false", arg.parse("--foo=false") );
        assertEquals( "true", arg.parse("--foo=true") );
        assertEquals( "true", arg.parse("--foo") );
    }

    @Test
    public void usageTest() throws Exception
    {
        OptionalBooleanArg arg = new OptionalBooleanArg( "foo", true, "" );

        assertEquals( "[--foo[=<true|false>]]", arg.usage() );
    }
}
