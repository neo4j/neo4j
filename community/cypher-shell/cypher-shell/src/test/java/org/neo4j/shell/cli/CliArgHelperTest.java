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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import org.neo4j.shell.test.LocaleDependentTestBase;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.shell.test.Util.asArray;

class CliArgHelperTest extends LocaleDependentTestBase
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
    void testForceNonInteractiveIsNotDefault()
    {
        assertFalse( parse( asArray() ).getNonInteractive(), "Force non-interactive should not be the default mode" );
    }

    @Test
    void testForceNonInteractiveIsParsed()
    {
        assertTrue( parse( asArray( "--non-interactive" ) ).getNonInteractive(), "Force non-interactive should have been parsed to true" );
    }

    @Test
    void testNumSampleRows()
    {
        assertEquals( 200, parse( "--sample-rows 200".split( " " ) ).getNumSampleRows(), "sample-rows 200" );
        assertNull( CliArgHelper.parse( "--sample-rows 0".split( " " ) ), "invalid sample-rows" );
        assertNull( CliArgHelper.parse( "--sample-rows -1".split( " " ) ), "invalid sample-rows" );
        assertNull( CliArgHelper.parse( "--sample-rows foo".split( " " ) ), "invalid sample-rows" );
    }

    @Test
    void testWrap()
    {
        assertTrue( parse( "--wrap true".split( " " ) ).getWrap(), "wrap true" );
        assertFalse( parse( "--wrap false".split( " " ) ).getWrap(), "wrap false" );
        assertTrue( parse().getWrap(), "default wrap" );
        assertNull( CliArgHelper.parse( "--wrap foo".split( " " ) ), "invalid wrap" );
    }

    @Test
    void testDefaultScheme()
    {
        CliArgs arguments = CliArgHelper.parse();
        assertNotNull( arguments );
        assertEquals( "neo4j", arguments.getScheme() );
    }

    @Test
    void testDebugIsNotDefault()
    {
        assertFalse( parse( asArray() ).getDebugMode(), "Debug should not be the default mode" );
    }

    @Test
    void testDebugIsParsed()
    {
        assertTrue( parse( asArray( "--debug" ) ).getDebugMode(), "Debug should have been parsed to true" );
    }

    @Test
    void testVersionIsParsed()
    {
        assertTrue( parse( asArray( "--version" ) ).getVersion(), "Version should have been parsed to true" );
    }

    @Test
    void testDriverVersionIsParsed()
    {
        assertTrue( parse( asArray( "--driver-version" ) ).getDriverVersion(), "Driver version should have been parsed to true" );
    }

    @Test
    void testFailFastIsDefault()
    {
        assertEquals( FailBehavior.FAIL_FAST, parse( asArray() ).getFailBehavior(), "Unexpected fail-behavior" );
    }

    @Test
    void testFailFastIsParsed()
    {
        assertEquals( FailBehavior.FAIL_FAST, parse( asArray( "--fail-fast" ) ).getFailBehavior(), "Unexpected fail-behavior" );
    }

    @Test
    void testFailAtEndIsParsed()
    {
        assertEquals( FailBehavior.FAIL_AT_END, parse( asArray( "--fail-at-end" ) ).getFailBehavior(), "Unexpected fail-behavior" );
    }

    @Test
    void singlePositionalArgumentIsFine()
    {
        String text = "Single string";
        assertEquals( Optional.of( text ), parse( asArray( text ) ).getCypher(), "Did not parse cypher string" );
    }

    @Test
    void parseArgumentsAndQuery()
    {
        String query = "\"match (n) return n\"";
        ArrayList<String> strings = new ArrayList<>( asList( "-a 192.168.1.1 -p 123 --format plain".split( " " ) ) );
        strings.add( query );
        assertEquals( Optional.of( query ), parse( strings.toArray( new String[0] ) ).getCypher() );
    }

    @Test
    void parseFormat()
    {
        assertEquals( Format.PLAIN, parse( "--format", "plain" ).getFormat() );
        assertEquals( Format.VERBOSE, parse( "--format", "verbose" ).getFormat() );
    }

    @Test
    void parsePassword()
    {
        assertEquals( "foo", parse( "--password", "foo" ).getPassword() );
    }

    @Test
    void parseUserName()
    {
        assertEquals( "foo", parse( "--username", "foo" ).getUsername() );
    }

    @Test
    void parseFullAddress()
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
    void defaultAddress()
    {
        CliArgs cliArgs = CliArgHelper.parse();
        assertNotNull( cliArgs );
        assertEquals( CliArgs.DEFAULT_SCHEME, cliArgs.getScheme() );
        assertEquals( CliArgs.DEFAULT_HOST, cliArgs.getHost() );
        assertEquals( CliArgs.DEFAULT_PORT, cliArgs.getPort() );
    }

    @Test
    void parseWithoutProtocol()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "localhost:10000" );
        assertNotNull( cliArgs );
        assertNotNull( cliArgs );
        assertEquals( "neo4j", cliArgs.getScheme() );
        assertEquals( "localhost", cliArgs.getHost() );
        assertEquals( 10000, cliArgs.getPort() );
    }

    @Test
    void parseAddressWithRoutingContext()
    {
        CliArgs cliArgs = CliArgHelper.parse( "--address", "neo4j://localhost:7697?policy=one" );
        assertNotNull( cliArgs );
        assertEquals( "neo4j", cliArgs.getScheme() );
        assertEquals( "localhost", cliArgs.getHost() );
        assertEquals( 7697, cliArgs.getPort() );
    }

    @Test
    void nonsenseArgsGiveError()
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        System.setErr( new PrintStream( bout ) );

        CliArgs cliargs = CliArgHelper.parse( "-notreally" );

        assertNull( cliargs );

        assertTrue( bout.toString().contains( "cypher-shell [-h]" ) );
        assertTrue( bout.toString().contains( "cypher-shell: error: unrecognized arguments: '-notreally'" ) );
    }

    @Test
    void nonsenseUrlGivesError()
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        System.setErr( new PrintStream( bout ) );

        CliArgs cliargs = CliArgHelper.parse( "--address", "host port" );

        assertNull( cliargs, "should have failed" );

        assertTrue(
                bout.toString().contains( "cypher-shell [-h]" ), "expected usage: " + bout );
        assertTrue(
                bout.toString().contains( "cypher-shell: error: Failed to parse address" ), "expected error: " + bout );
        assertTrue(
                bout.toString().contains( "\n  Address should be of the form:" ), "expected error detail: " + bout );
    }

    @Test
    void defaultsEncryptionToDefault()
    {
        assertEquals( Encryption.DEFAULT, parse().getEncryption() );
    }

    @Test
    void allowsEncryptionToBeTurnedOnOrOff()
    {
        assertEquals( Encryption.TRUE, parse( "--encryption", "true" ).getEncryption() );
        assertEquals( Encryption.FALSE, parse( "--encryption", "false" ).getEncryption() );
    }

    @Test
    void shouldNotAcceptInvalidEncryption()
    {
        var exception = assertThrows( ArgumentParserException.class, () ->  CliArgHelper.parseAndThrow( "--encryption", "bugaluga" ) );
        assertThat( exception.getMessage(), containsString( "argument --encryption: invalid choice: 'bugaluga' (choose from {true,false,default})" ) );
    }

    @Test
    void shouldParseSingleIntegerArgWithAddition()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>3+5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    void shouldParseSingleIntegerArgWithAdditionAndWhitespace()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo => 3 + 5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    void shouldParseWithSpaceSyntax()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo 3+5" );
        assertNotNull( cliArgs );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    void shouldParseSingleStringArg()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>'nanana'" );
        assertNotNull( cliArgs );
        assertEquals( "nanana", cliArgs.getParameters().allParameterValues().get( "foo" ) );
    }

    @Test
    void shouldParseTwoArgs()
    {
        CliArgs cliArgs = CliArgHelper.parse( "-P", "foo=>'nanana'", "-P", "bar=>3+5" );
        assertNotNull( cliArgs );
        assertEquals( "nanana", cliArgs.getParameters().allParameterValues().get( "foo" ) );
        assertEquals( 8L, cliArgs.getParameters().allParameterValues().get( "bar" ) );
    }

    @Test
    void shouldFailForInvalidSyntaxForArg()
    {
        var exception = assertThrows( ArgumentParserException.class, () -> CliArgHelper.parseAndThrow( "-P", "foo: => 'nanana'" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect usage" ) );
        assertThat( exception.getMessage(), containsString( "usage: --param  \"name => value\"" ) );
    }

    @Test
    void testDefaultInputFileName()
    {
        CliArgs arguments = CliArgHelper.parse();
        assertNotNull( arguments );
        assertNull( arguments.getInputFilename() );
    }

    @Test
    void testSetInputFileName()
    {
        CliArgs arguments = CliArgHelper.parse( "--file", "foo" );
        assertNotNull( arguments );
        assertEquals( "foo", arguments.getInputFilename() );
    }

    @Test
    void helpfulIfUsingWrongFile()
    {
        var exception = assertThrows( ArgumentParserException.class, () -> CliArgHelper.parseAndThrow( "-file", "foo" ) );
        assertThat( exception.getMessage(), containsString( "Unrecognized argument '-file', did you mean --file?" ) );
    }
}
