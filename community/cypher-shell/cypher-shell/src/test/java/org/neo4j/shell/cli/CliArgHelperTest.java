/*
 * Copyright (c) "Neo4j"
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
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import org.neo4j.shell.test.LocaleDependentTestBase;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.shell.test.Util.asArray;

public class CliArgHelperTest extends LocaleDependentTestBase
{
    private static CliArgs parse( String... args )
    {
        var parsed = CliArgHelper.parse( args );
        if ( parsed == null )
        {
            fail( "Failed to parse arguments: " + Arrays.toString( args ) );
        }
        return parsed;
    }

    @Test
    public void testForceNonInteractiveIsNotDefault()
    {
        assertFalse( "Force non-interactive should not be the default mode", parse( asArray() ).getNonInteractive() );
    }

    @Test
    public void testForceNonInteractiveIsParsed()
    {
        assertTrue( "Force non-interactive should have been parsed to true", parse( asArray( "--non-interactive" ) ).getNonInteractive() );
    }

    @Test
    public void testNumSampleRows()
    {
        assertEquals( "sample-rows 200", 200, parse( "--sample-rows 200".split( " " ) ).getNumSampleRows() );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows 0".split( " " ) ) );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows -1".split( " " ) ) );
        assertNull( "invalid sample-rows", CliArgHelper.parse( "--sample-rows foo".split( " " ) ) );
    }

    @Test
    public void testWrap()
    {
        assertTrue( "wrap true", parse( "--wrap true".split( " " ) ).getWrap() );
        assertFalse( "wrap false", parse( "--wrap false".split( " " ) ).getWrap() );
        assertTrue( "default wrap", parse().getWrap() );
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
        assertFalse( "Debug should not be the default mode", parse( asArray() ).getDebugMode() );
    }

    @Test
    public void testDebugIsParsed()
    {
        assertTrue( "Debug should have been parsed to true", parse( asArray( "--debug" ) ).getDebugMode() );
    }

    @Test
    public void testVersionIsParsed()
    {
        assertTrue( "Version should have been parsed to true", parse( asArray( "--version" ) ).getVersion() );
    }

    @Test
    public void testDriverVersionIsParsed()
    {
        assertTrue( "Driver version should have been parsed to true", parse( asArray( "--driver-version" ) ).getDriverVersion() );
    }

    @Test
    public void testFailFastIsDefault()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_FAST, parse( asArray() ).getFailBehavior() );
    }

    @Test
    public void testFailFastIsParsed()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_FAST, parse( asArray( "--fail-fast" ) ).getFailBehavior() );
    }

    @Test
    public void testFailAtEndIsParsed()
    {
        assertEquals( "Unexpected fail-behavior", FailBehavior.FAIL_AT_END, parse( asArray( "--fail-at-end" ) ).getFailBehavior() );
    }

    @Test
    public void singlePositionalArgumentIsFine()
    {
        String text = "Single string";
        assertEquals( "Did not parse cypher string", Optional.of( text ), parse( asArray( text ) ).getCypher() );
    }

    @Test
    public void parseArgumentsAndQuery()
    {
        String query = "\"match (n) return n\"";
        ArrayList<String> strings = new ArrayList<>( asList( "-a 192.168.1.1 -p 123 --format plain".split( " " ) ) );
        strings.add( query );
        assertEquals( Optional.of( query ), parse( strings.toArray( new String[0] ) ).getCypher() );
    }

    @Test
    public void parseFormat()
    {
        assertEquals( Format.PLAIN, parse( "--format", "plain" ).getFormat() );
        assertEquals( Format.VERBOSE, parse( "--format", "verbose" ).getFormat() );
    }

    @Test
    public void parsePassword()
    {
        assertEquals( "foo", parse( "--password", "foo" ).getPassword() );
    }

    @Test
    public void parseUserName()
    {
        assertEquals( "foo", parse( "--username", "foo" ).getUsername() );
    }

    @Test
    public void parseFullAddress()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "bolt+routing://alice:foo@bar:69" );
        assertNotNull( cliArgs );
        assertEquals( "alice", cliArgs.getUsername() );
        assertEquals( "foo", cliArgs.getPassword() );
        assertEquals( "bolt+routing", cliArgs.getScheme() );
        assertEquals( "bar", cliArgs.getHost() );
        assertEquals( 69, cliArgs.getPort() );
    }

    @Test
    public void defaultAddress()
    {
        CliArgs cliArgs = CliArgHelper.parse();
        assertNotNull( cliArgs );
        assertEquals( CliArgs.DEFAULT_SCHEME, cliArgs.getScheme() );
        assertEquals( CliArgs.DEFAULT_HOST, cliArgs.getHost() );
        assertEquals( CliArgs.DEFAULT_PORT, cliArgs.getPort() );
    }

    @Test
    public void parseWithoutProtocol()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "localhost:10000" );
        assertNotNull( cliArgs );
        assertNotNull( cliArgs );
        assertEquals( "neo4j", cliArgs.getScheme() );
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
        assertEquals( Encryption.DEFAULT, parse().getEncryption() );
    }

    @Test
    public void allowsEncryptionToBeTurnedOnOrOff()
    {
        assertEquals( Encryption.TRUE, parse( "--encryption", "true" ).getEncryption() );
        assertEquals( Encryption.FALSE, parse( "--encryption", "false" ).getEncryption() );
    }

    @Test
    public void shouldNotAcceptInvalidEncryption()
    {
        var exception = assertThrows( ArgumentParserException.class, () ->  CliArgHelper.parseAndThrow( "--encryption", "bugaluga" ) );
        assertThat( exception.getMessage(), containsString( "argument --encryption: invalid choice: 'bugaluga' (choose from {true,false,default})" ) );
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
    public void shouldFailForInvalidSyntaxForArg()
    {
        var exception = assertThrows( ArgumentParserException.class, () -> CliArgHelper.parseAndThrow( "-P", "foo: => 'nanana'" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect usage" ) );
        assertThat( exception.getMessage(), containsString( "usage: --param  \"name => value\"" ) );
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

    @Test
    public void helpfulIfUsingWrongFile()
    {
        var exception = assertThrows( ArgumentParserException.class, () -> CliArgHelper.parseAndThrow( "-file", "foo" ) );
        assertThat( exception.getMessage(), containsString( "Unrecognized argument '-file', did you mean --file?" ) );
    }
}
