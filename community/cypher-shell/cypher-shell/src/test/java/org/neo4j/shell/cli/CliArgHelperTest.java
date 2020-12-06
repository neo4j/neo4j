/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.cli;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.shell.test.Util.asArray;

public class CliArgHelperTest
{

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private PrintStream mockedStdErr;

    @Before
    public void setup()
    {
        mockedStdErr = mock( PrintStream.class );
    }

    @Test
    public void testForceNonInteractiveIsNotDefault()
    {
        assertFalse( "Force non-interactive should not be the default mode",
                     CliArgHelper.parse( asArray() ).getNonInteractive() );
    }

    @Test
    public void testForceNonInteractiveIsParsed()
    {
        assertTrue( "Force non-interactive should have been parsed to true",
                    CliArgHelper.parse( asArray( "--non-interactive" ) ).getNonInteractive() );
    }

    @Test
    public void testNumSampleRows()
    {
        assertEquals( "sample-rows 200", 200, CliArgHelper.parse( "--sample-rows 200".split( " " ) ).getNumSampleRows() );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows 0".split( " " ) ) );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows -1".split( " " ) ) );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows foo".split( " " ) ) );
    }

    @Test
    public void testWrap()
    {
        assertTrue( "wrap true", CliArgHelper.parse( "--wrap true".split( " " ) ).getWrap() );
        assertFalse( "wrap false", CliArgHelper.parse( "--wrap false".split( " " ) ).getWrap() );
        assertTrue( "default wrap", CliArgHelper.parse().getWrap() );
        assertNull( "invalid wrap", CliArgHelper.parse( "--wrap foo".split( " " ) ) );
    }

    @Test
    public void testDefaultScheme()
    {
        CliArgs arguments = CliArgHelper.parse();
        assertNotNull( arguments );
        assertEquals( "neo4j", arguments.getScheme() );
    }

    @Test
    public void testDebugIsNotDefault()
    {
        assertFalse( "Debug should not be the default mode",
                     CliArgHelper.parse( asArray() ).getDebugMode() );
    }

    @Test
    public void testDebugIsParsed()
    {
        assertTrue( "Debug should have been parsed to true",
                    CliArgHelper.parse( asArray( "--debug" ) ).getDebugMode() );
    }

    @Test
    public void testVersionIsParsed()
    {
        assertTrue( "Version should have been parsed to true",
                    CliArgHelper.parse( asArray( "--version" ) ).getVersion() );
    }

    @Test
    public void testDriverVersionIsParsed()
    {
        assertTrue( "Driver version should have been parsed to true",
                    CliArgHelper.parse( asArray( "--driver-version" ) ).getDriverVersion() );
    }

    @Test
    public void testFailFastIsDefault()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_FAST,
                      CliArgHelper.parse( asArray() ).getFailBehavior() );
    }

    @Test
    public void testFailFastIsParsed()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_FAST,
                      CliArgHelper.parse( asArray( "--fail-fast" ) ).getFailBehavior() );
    }

    @Test
    public void testFailAtEndIsParsed()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_AT_END,
                      CliArgHelper.parse( asArray( "--fail-at-end" ) ).getFailBehavior() );
    }

    @Test
    public void singlePositionalArgumentIsFine()
    {
        String text = "Single string";
        assertEquals( "Did not parse cypher string", text,
                      CliArgHelper.parse( asArray( text ) ).getCypher().get() );
    }

    @Test
    public void parseArgumentsAndQuery()
    {
        String query = "\"match (n) return n\"";
        ArrayList<String> strings = new ArrayList<>();
        strings.addAll( asList( "-a 192.168.1.1 -p 123 --format plain".split( " " ) ) );
        strings.add( query );
        assertEquals( Optional.of( query ),
                      CliArgHelper.parse( strings.toArray( new String[strings.size()] ) ).getCypher() );
    }

    @Test
    public void parseFormat()
    {
        assertEquals( Format.PLAIN, CliArgHelper.parse( "--format", "plain" ).getFormat() );
        assertEquals( Format.VERBOSE, CliArgHelper.parse( "--format", "verbose" ).getFormat() );
    }

    @Test
    public void parsePassword()
    {
        assertEquals( "foo", CliArgHelper.parse( "--password", "foo" ).getPassword() );
    }

    @Test
    public void parseUserName()
    {
        assertEquals( "foo", CliArgHelper.parse( "--username", "foo" ).getUsername() );
    }

    @Test
    public void parseFullAddress()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "bolt+routing://alice:foo@bar:69" );
        assertNotNull( cliArgs );
        assertEquals( "alice", cliArgs.getUsername() );
        assertEquals( "foo", cliArgs.getPassword() );
        assertEquals( "bar", cliArgs.getHost() );
        assertEquals( 69, cliArgs.getPort() );
    }

    @Test
    public void parseWithoutProtocol()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "localhost:10000" );
        assertNotNull( cliArgs );
        assertNotNull( cliArgs );
        assertEquals( "bolt", cliArgs.getScheme() );
        assertEquals( "localhost", cliArgs.getHost() );
        assertEquals( 10000, cliArgs.getPort() );
    }

    @Test
    public void parseAddressWithRoutingContext()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "neo4j://localhost:7697?policy=one" );
        assertNotNull( cliArgs );
        assertEquals( "neo4j", cliArgs.getScheme() );
        assertEquals( "localhost", cliArgs.getHost() );
        assertEquals( 7697, cliArgs.getPort() );
    }

    @Test
    public void nonsenseArgsGiveError()
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        System.setErr( new PrintStream( bout ) );

        CliArgs cliargs = CliArgHelper.parse( "-notreally" );

        assertNull( cliargs );

        assertTrue( bout.toString().contains( "cypher-shell [-h]" ) );
        assertTrue( bout.toString().contains( "cypher-shell: error: unrecognized arguments: '-notreally'" ) );
    }

    @Test
    public void nonsenseUrlGivesError()
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        System.setErr( new PrintStream( bout ) );

        CliArgs cliargs = CliArgHelper.parse( "--address", "host port" );

        assertNull( "should have failed", cliargs );

        assertTrue( "expected usage: " + bout.toString(),
                    bout.toString().contains( "cypher-shell [-h]" ) );
        assertTrue( "expected error: " + bout.toString(),
                    bout.toString().contains( "cypher-shell: error: Failed to parse address" ) );
        assertTrue( "expected error detail: " + bout.toString(),
                    bout.toString().contains( "\n  Address should be of the form:" ) );
    }

    @Test
    public void defaultsEncryptionToDefault()
    {
        assertEquals( Encryption.DEFAULT, CliArgHelper.parse().getEncryption() );
    }

    @Test
    public void allowsEncryptionToBeTurnedOnOrOff()
    {
        assertEquals( Encryption.TRUE, CliArgHelper.parse( "--encryption", "true" ).getEncryption() );
        assertEquals( Encryption.FALSE, CliArgHelper.parse( "--encryption", "false" ).getEncryption() );
    }

    @Test
    public void shouldNotAcceptInvalidEncryption() throws Exception
    {
        thrown.expect( ArgumentParserException.class );
        thrown.expectMessage( containsString( "argument --encryption: invalid choice: 'bugaluga' (choose from {true,false,default})" ) );
        CliArgHelper.parseAndThrow( "--encryption", "bugaluga" );
    }

    @Test
    public void shouldParseSingleIntegerArgWithAddition()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>3+5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    public void shouldParseSingleIntegerArgWithAdditionAndWhitespace()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo => 3 + 5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    public void shouldParseWithSpaceSyntax()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo 3+5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    public void shouldParseSingleStringArg()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>'nanana'" );
        assertNotNull( cliArgs );
        assertEquals( "nanana", cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    public void shouldParseTwoArgs()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>'nanana'", "-P", "bar=>3+5" );
        assertNotNull( cliArgs );
        assertEquals( "nanana", cliArgs.getParameters().allParameterValues().get( "foo" ) );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "bar" ) );
    }

    @Test
    public void shouldFailForInvalidSyntaxForArg() throws Exception
    {
        thrown.expect( ArgumentParserException.class );
        thrown.expectMessage( allOf(
                containsString( "Incorrect usage" ),
                containsString( "usage: --param  \"name => value\"" ) ) );
        CliArgHelper.parseAndThrow( "-P", "foo: => 'nanana'" );
    }

    @Test
    public void testDefaultInputFileName()
    {
        CliArgs arguments = CliArgHelper.parse();
        assertNotNull( arguments );
        assertNull( arguments.getInputFilename() );
    }

    @Test
    public void testSetInputFileName()
    {
        CliArgs arguments = CliArgHelper.parse( "--file", "foo" );
        assertNotNull( arguments );
        assertEquals( "foo", arguments.getInputFilename() );
    }
}
