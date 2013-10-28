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
    
    @Test
    public void testParameterWithDashValue()
    {
        String [] line = { "-file", "-" };
        Args args = new Args ( line );
        assertEquals( 1, args.asMap().size() );
        assertEquals( "-", args.get ( "file", null ) );
        assertTrue( args.orphans().isEmpty() );
    }
    
    @Test
    public void testEnum()
    {
        String[] line = { "--enum=" + MyEnum.second.name() };
        Args args = new Args( line );
        Enum<MyEnum> result = args.getEnum( MyEnum.class, "enum", MyEnum.first );
        assertEquals( MyEnum.second, result );
    }
        
    @Test
    public void testEnumWithDefault()
    {
        String[] line = {};
        Args args = new Args( line );
        MyEnum result = args.getEnum( MyEnum.class, "enum", MyEnum.third );
        assertEquals( MyEnum.third, result );
    }
    
    @Test( expected = IllegalArgumentException.class )
    public void testEnumWithInvalidValue() throws Exception
    {
        String[] line = { "--myenum=something" };
        Args args = new Args( line );
        args.getEnum( MyEnum.class, "myenum", MyEnum.third );
    }
    
    private static enum MyEnum
    {
        first,
        second,
        third;
    }
}
