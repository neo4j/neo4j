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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreConstArgumentAction;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.impl.choice.CollectionArgumentChoice;
import net.sourceforge.argparse4j.impl.type.BooleanArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.ParameterMap;

import static java.lang.String.format;
import static org.neo4j.shell.cli.CliArgs.DEFAULT_SCHEME;
import static org.neo4j.shell.cli.FailBehavior.FAIL_AT_END;
import static org.neo4j.shell.cli.FailBehavior.FAIL_FAST;

/**
 * Command line argument parsing and related stuff
 */
public class CliArgHelper
{

    /**
     * @param args to parse
     * @return null in case of error, commandline arguments otherwise
     */
    @Nullable
    public static CliArgs parse( @Nonnull String... args )
    {
        final CliArgs cliArgs = new CliArgs();

        final ArgumentParser parser = setupParser( cliArgs.getParameters() );
        final Namespace ns;

        try
        {
            ns = parser.parseArgs( args );
        }
        catch ( ArgumentParserException e )
        {
            parser.handleError( e );
            return null;
        }
        return getCliArgs( cliArgs, parser, ns );
    }

    /**
     * @param args to parse
     * @return commandline arguments
     * @throws ArgumentParserException if an argument can't be parsed.
     */
    public static CliArgs parseAndThrow( @Nonnull String... args ) throws ArgumentParserException
    {
        final CliArgs cliArgs = new CliArgs();
        final ArgumentParser parser = setupParser( cliArgs.getParameters() );
        final Namespace ns = parser.parseArgs( args );
        return getCliArgs( cliArgs, parser, ns );
    }

    private static CliArgs getCliArgs( CliArgs cliArgs, ArgumentParser parser, Namespace ns )
    {
        // Parse address string, returns null on error
        final URI uri = parseURI( parser, ns.getString( "address" ) );

        if ( uri == null )
        {
            return null;
        }

        //---------------------
        // Connection arguments
        cliArgs.setScheme( uri.getScheme(), "bolt" );
        cliArgs.setHost( uri.getHost(), "localhost" );

        int port = uri.getPort();
        cliArgs.setPort( port == -1 ? 7687 : port );
        // Also parse username and password from address if available
        parseUserInfo( uri, cliArgs );

        // Only overwrite user/pass from address string if the arguments were specified
        String user = ns.getString( "username" );
        if ( !user.isEmpty() )
        {
            cliArgs.setUsername( user, cliArgs.getUsername() );
        }
        String pass = ns.getString( "password" );
        if ( !pass.isEmpty() )
        {
            cliArgs.setPassword( pass, cliArgs.getPassword() );
        }
        cliArgs.setEncryption( Encryption.parse( ns.get( "encryption" ) ) );
        cliArgs.setDatabase( ns.getString( "database" ) );
        cliArgs.setInputFilename( ns.getString( "file" ) );

        //----------------
        // Other arguments
        // cypher string might not be given, represented by null
        cliArgs.setCypher( ns.getString( "cypher" ) );
        // Fail behavior as sensible default and returns a proper type
        cliArgs.setFailBehavior( ns.get( "fail-behavior" ) );

        //Set Output format
        cliArgs.setFormat( Format.parse( ns.get( "format" ) ) );

        cliArgs.setDebugMode( ns.getBoolean( "debug" ) );

        cliArgs.setNonInteractive( ns.getBoolean( "force-non-interactive" ) );

        cliArgs.setWrap( ns.getBoolean( "wrap" ) );

        cliArgs.setNumSampleRows( ns.getInt( "sample-rows" ) );

        cliArgs.setVersion( ns.getBoolean( "version" ) );

        cliArgs.setDriverVersion( ns.getBoolean( "driver-version" ) );

        return cliArgs;
    }

    private static void parseUserInfo( URI uri, CliArgs cliArgs )
    {
        String userInfo = uri.getUserInfo();
        String user = null;
        String password = null;
        if ( userInfo != null )
        {
            String[] split = userInfo.split( ":" );
            if ( split.length == 0 )
            {
                user = userInfo;
            }
            else if ( split.length == 2 )
            {
                user = split[0];
                password = split[1];
            }
            else
            {
                throw new IllegalArgumentException( "Cannot parse user and password from " + userInfo );
            }
        }
        cliArgs.setUsername( user, "" );
        cliArgs.setPassword( password, "" );
    }

    @Nullable
    static URI parseURI( ArgumentParser parser, String address )
    {
        try
        {
            String[] schemeSplit = address.split( "://" );
            if ( schemeSplit.length == 1 )
            {
                // URI can't parse addresses without scheme, prepend fake "bolt://" to reuse the parsing facility
                address = DEFAULT_SCHEME + "://" + address;
            }
            return new URI( address );
        }
        catch ( URISyntaxException e )
        {
            PrintWriter printWriter = new PrintWriter( System.err );
            parser.printUsage( printWriter );
            printWriter.println( "cypher-shell: error: Failed to parse address: '" + address + "'" );
            printWriter.println( "\n  Address should be of the form: [scheme://][username:password@][host][:port]" );
            printWriter.flush();
            return null;
        }
    }

    private static ArgumentParser setupParser( ParameterMap parameterMap )
    {
        ArgumentParser parser = ArgumentParsers.newArgumentParser( "cypher-shell" ).defaultHelp( true ).description(
                format( "A command line shell where you can execute Cypher against an instance of Neo4j. " +
                        "By default the shell is interactive but you can use it for scripting by passing cypher " +
                        "directly on the command line or by piping a file with cypher statements (requires Powershell on Windows)." +
                        "%n%n" +
                        "example of piping a file:%n" +
                        "  cat some-cypher.txt | cypher-shell" ) );

        ArgumentGroup connGroup = parser.addArgumentGroup( "connection arguments" );
        connGroup.addArgument( "-a", "--address" )
                 .help( "address and port to connect to" )
                 .setDefault( String.format( "%s://%s:%d", CliArgs.DEFAULT_SCHEME, CliArgs.DEFAULT_HOST, CliArgs.DEFAULT_PORT ) );
        connGroup.addArgument( "-u", "--username" )
                 .setDefault( "" )
                 .help( "username to connect as. Can also be specified using environment variable " + ConnectionConfig.USERNAME_ENV_VAR );
        connGroup.addArgument( "-p", "--password" )
                 .setDefault( "" )
                 .help( "password to connect with. Can also be specified using environment variable " + ConnectionConfig.PASSWORD_ENV_VAR );
        connGroup.addArgument( "--encryption" )
                 .help( "whether the connection to Neo4j should be encrypted. This must be consistent with Neo4j's " +
                        "configuration. If choosing '" + Encryption.DEFAULT.name().toLowerCase() +
                        "' the encryption setting is deduced from the specified address. " +
                        "For example the 'neo4j+ssc' protocol would use encryption." )
                 .choices( new CollectionArgumentChoice<>(
                         Encryption.TRUE.name().toLowerCase(),
                         Encryption.FALSE.name().toLowerCase(),
                         Encryption.DEFAULT.name().toLowerCase() ) )
                 .setDefault( Encryption.DEFAULT.name().toLowerCase() );
        connGroup.addArgument( "-d", "--database" )
                 .help( "database to connect to. Can also be specified using environment variable " + ConnectionConfig.DATABASE_ENV_VAR )
                 .setDefault( "" );

        MutuallyExclusiveGroup failGroup = parser.addMutuallyExclusiveGroup();
        failGroup.addArgument( "--fail-fast" )
                 .help( "exit and report failure on first error when reading from file (this is the default behavior)" )
                 .dest( "fail-behavior" )
                 .setConst( FAIL_FAST )
                 .action( new StoreConstArgumentAction() );
        failGroup.addArgument( "--fail-at-end" )
                 .help( "exit and report failures at end of input when reading from file" )
                 .dest( "fail-behavior" )
                 .setConst( FAIL_AT_END )
                 .action( new StoreConstArgumentAction() );
        parser.setDefault( "fail-behavior", FAIL_FAST );

        parser.addArgument( "--format" )
              .help( "desired output format, verbose displays results in tabular format and prints statistics, " +
                     "plain displays data with minimal formatting" )
              .choices( new CollectionArgumentChoice<>(
                      Format.AUTO.name().toLowerCase(),
                      Format.VERBOSE.name().toLowerCase(),
                      Format.PLAIN.name().toLowerCase() ) )
              .setDefault( Format.AUTO.name().toLowerCase() );

        parser.addArgument( "-P", "--param" )
              .help( "Add a parameter to this session. Example: `-P \"number => 3\"`. This argument can be specified multiple times." )
              .action( new AddParamArgumentAction( parameterMap ) );

        parser.addArgument( "--debug" )
              .help( "print additional debug information" )
              .action( new StoreTrueArgumentAction() );

        parser.addArgument( "--non-interactive" )
              .help( "force non-interactive mode, only useful if auto-detection fails (like on Windows)" )
              .dest( "force-non-interactive" )
              .action( new StoreTrueArgumentAction() );

        parser.addArgument( "--sample-rows" )
              .help( "number of rows sampled to compute table widths (only for format=VERBOSE)" )
              .type( new PositiveIntegerType() )
              .dest( "sample-rows" )
              .setDefault( CliArgs.DEFAULT_NUM_SAMPLE_ROWS );

        parser.addArgument( "--wrap" )
              .help( "wrap table column values if column is too narrow (only for format=VERBOSE)" )
              .type( new BooleanArgumentType() )
              .setDefault( true );

        parser.addArgument( "-v", "--version" )
              .help( "print version of cypher-shell and exit" )
              .action( new StoreTrueArgumentAction() );

        parser.addArgument( "--driver-version" )
              .help( "print version of the Neo4j Driver used and exit" )
              .dest( "driver-version" )
              .action( new StoreTrueArgumentAction() );

        parser.addArgument( "cypher" )
              .nargs( "?" )
              .help( "an optional string of cypher to execute and then exit" );
        parser.addArgument( "-f", "--file" )
              .help( "Pass a file with cypher statements to be executed. After the statements have been executed cypher-shell will be shutdown" );

        return parser;
    }

    private static class PositiveIntegerType implements ArgumentType<Integer>
    {
        @Override
        public Integer convert( ArgumentParser parser, Argument arg, String value ) throws ArgumentParserException
        {
            try
            {
                int result = Integer.parseInt( value );
                if ( result < 1 )
                {
                    throw new NumberFormatException( value );
                }
                return result;
            }
            catch ( NumberFormatException nfe )
            {
                throw new ArgumentParserException( "Invalid value: " + value, parser );
            }
        }
    }
}
