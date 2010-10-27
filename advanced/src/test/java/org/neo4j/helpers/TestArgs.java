package org.neo4j.helpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestArgs
{
    @Test
    public void testInterleavedParametersWithValuesAndNot()
    {
        String[] line = { "-host", "machine.foo.com", "-port", "1234", "-v", "-name", "othershell" };
        Args args = new Args( line );
        assertEquals( "machine.foo.com", args.get( "host", null ) );
        assertEquals( "1234", args.get( "port", null ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "othershell", args.get( "name", null ) );
        assertTrue( args.has( "v" ) );
        assertTrue( args.orphans().isEmpty() );
    }
    
    @Test
    public void testInterleavedEqualsArgsAndSplitKeyValue()
    {
        String[] line = { "-host=localhost", "-v", "--port", "1234", "param1", "-name=Something", "param2" };
        Args args = new Args( line );
        assertEquals( "localhost", args.get( "host", null ) );
        assertTrue( args.has( "v" ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "Something", args.get( "name", null ) );
        
        assertEquals( 2, args.orphans().size() );
        assertEquals( "param1", args.orphans().get( 0 ) );
        assertEquals( "param2", args.orphans().get( 1 ) );
    }
}
