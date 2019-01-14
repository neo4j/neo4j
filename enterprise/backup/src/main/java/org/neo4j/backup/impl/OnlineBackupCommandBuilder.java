/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.ParameterisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static java.lang.String.format;

/**
 * This class encapsulates the information needed to perform an online backup against a running Neo4j instance
 * configured to act as a backup server.
 * <p>
 * All backup methods return the same instance, allowing for chaining calls.
 */
public class OnlineBackupCommandBuilder
{
    private String host;
    private Integer port;
    private Boolean fallbackToFull;
    private Long timeout;
    private Boolean checkConsistency;
    private File consistencyReportLocation;
    private Config additionalConfig;
    private SelectedBackupProtocol selectedBackupProtocol;
    private Boolean consistencyCheckGraph;
    private Boolean consistencyCheckIndexes;
    private Boolean consistencyCheckLabel;
    private Boolean consistencyCheckOwners;
    private OutputStream output;
    private Optional<String[]> rawArgs = Optional.empty();

    public OnlineBackupCommandBuilder withRawArgs( String... args )
    {
        rawArgs = Optional.of( args );
        return this;
    }

    public OnlineBackupCommandBuilder withHost( String host )
    {
        this.host = host;
        return this;
    }

    public OnlineBackupCommandBuilder withPort( Integer port )
    {
        this.port = port;
        return this;
    }

    public OnlineBackupCommandBuilder withFallbackToFull( Boolean flag )
    {
        this.fallbackToFull = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withTimeout( Long timeoutInMillis )
    {
        this.timeout = timeoutInMillis;
        return this;
    }

    public OnlineBackupCommandBuilder withConsistencyCheck( Boolean flag )
    {
        this.checkConsistency = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withReportFlag( File consistencyReportLocation )
    {
        this.consistencyReportLocation = consistencyReportLocation;
        return this;
    }

    public OnlineBackupCommandBuilder withAdditionalConfig( Config additionalConfig )
    {
        this.additionalConfig = additionalConfig;
        return this;
    }

    public OnlineBackupCommandBuilder withGraphConsistencyCheck( Boolean flag )
    {
        this.consistencyCheckGraph = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withIndexConsistencyCheck( Boolean flag )
    {
        this.consistencyCheckIndexes = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withLabelConsistencyCheck( Boolean flag )
    {
        this.consistencyCheckLabel = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withOwnerConsistencyCheck( Boolean flag )
    {
        this.consistencyCheckOwners = flag;
        return this;
    }

    public OnlineBackupCommandBuilder withOutput( OutputStream outputStream )
    {
        this.output = outputStream;
        return this;
    }

    public OnlineBackupCommandBuilder withSelectedBackupStrategy( SelectedBackupProtocol selectedBackupStrategy )
    {
        this.selectedBackupProtocol = selectedBackupStrategy;
        return this;
    }

    public boolean backup( File neo4jHome, String backupName ) throws CommandFailed, IncorrectUsage
    {
        File targetLocation = new File( neo4jHome, backupName );
        String[] args;
        if ( rawArgs.isPresent() )
        {
            args = rawArgs.get();
        }
        else
        {
            try
            {
                args = resolveArgs( targetLocation );
            }
            catch ( IOException e )
            {
                throw new CommandFailed( "Failed to resolve arguments", e );
            }
        }
        new OnlineBackupCommandProvider()
                .create( neo4jHome.toPath(),
                        configDirFromTarget( neo4jHome.toPath() ),
                        resolveOutsideWorld() )
                .execute( args );
        return true;
    }

    public String[] resolveArgs( File targetLocation ) throws IOException
    {
        return args(
                argBackupName( targetLocation ),
                argBackupLocation( targetLocation ),
                argFrom(),
                argFallbackToFull(),
                argSelectedProtocol(),
                argTimeout(),
                argCheckConsistency(),
                argReportDir(),
                argAdditionalConf( targetLocation ),
                argCcGraph(),
                argCcIndexes(),
                argCcLabel(),
                argCcOwners() );
    }

    private OutsideWorld resolveOutsideWorld()
    {
        Optional<OutputStream> output = Optional.ofNullable( this.output );
        return new ParameterisedOutsideWorld(
                System.console(), output.orElse( System.out ),
                output.orElse( System.err ),
                System.in, new DefaultFileSystemAbstraction() );
    }

    /**
     * Client handles the ports and hosts automatically, so no necessary need to specify
     * @return command line parameter for specifying the backup address
     */
    private String argFrom()
    {
        if ( host == null && port == null )
        {
            return "";
        }
        String address = String.join( ":",
                Optional.ofNullable( host ).orElse( "" ),
                Optional.ofNullable( port ).map( port -> Integer.toString( port ) ).orElse( "" ) );
        return format( "--from=%s", address );
    }

    /**
     * The backup location is the directory that stores multiple backups. Each directory in "backup location" is named after the backup name.
     * In order for a backup to belong where the user wants it, the backup location is the parent of the target specified by the user.
     * @return backup location command line argument
     */
    private String argBackupLocation( File targetLocation )
    {
        String location = Optional.ofNullable( targetLocation )
                .map( f -> targetLocation.getParentFile() )
                .orElseThrow( wrongArguments( "No target location specified" ) )
                .toString();
        return format( "--backup-dir=%s", location );
    }

    private String argBackupName( File targetLocation )
    {
        String backupName = Optional.ofNullable( targetLocation )
                .map( File::getName )
                .orElseThrow( wrongArguments( "No target location specified" ) );
        return format( "--name=%s", backupName );
    }

    private static Supplier<IllegalArgumentException> wrongArguments( String message )
    {
        return () -> new IllegalArgumentException( message );
    }

    private String argFallbackToFull()
    {
        return Optional.ofNullable( fallbackToFull )
                .map( flag -> format( "--fallback-to-full=%s", flag ) )
                .orElse( "" );
    }

    private String argSelectedProtocol()
    {
        return Optional.ofNullable( selectedBackupProtocol )
                .map( SelectedBackupProtocol::getName )
                .map( argValue -> format( "--%s=%s", OnlineBackupContextBuilder.ARG_NAME_PROTO_OVERRIDE, argValue ) )
                .orElse( "" );
    }

    private String argTimeout()
    {
        return Optional.ofNullable( this.timeout )
                .map( value -> format("--timeout=%dms", value) )
                .orElse( "" );
    }

    private String argCcOwners()
    {
        return Optional.ofNullable( this.consistencyCheckOwners )
                .map( value -> format( "--check-consistency=%b", this.consistencyCheckOwners ) )
                .orElse( "" );
    }

    private String argCcLabel()
    {
        return Optional.ofNullable( this.consistencyCheckLabel )
                .map( value -> format( "--cc-label-scan-store=%b", this.consistencyCheckLabel ) )
                .orElse( "" );
    }

    private String argCcIndexes()
    {
        return Optional.ofNullable( this.consistencyCheckIndexes )
                .map( value -> format( "--cc-indexes=%b", this.consistencyCheckIndexes ) )
                .orElse( "" );
    }

    private String argCcGraph()
    {
        return Optional.ofNullable( this.consistencyCheckGraph )
                .map( value -> format( "--cc-graph=%b", this.consistencyCheckGraph ) )
                .orElse( "" );
    }

    private String argAdditionalConf( File backupTarget ) throws IOException
    {
        if ( additionalConfig == null )
        {
            return "";
        }
        File configFile = backupTarget.toPath().resolve( "../additional_neo4j.conf" ).toFile();
        writeConfigToFile( additionalConfig, configFile );

        return format( "--additional-config=%s", configFile );
    }

    static void writeConfigToFile( Config config, File file ) throws IOException
    {
        try ( Writer fileWriter = new BufferedWriter( new FileWriter( file ) ) )
        {
            for ( Map.Entry<String,String> entry : config.getRaw().entrySet() )
            {
                fileWriter.write( format( "%s=%s\n", entry.getKey(), entry.getValue() ) );
            }
        }
    }

    private String argReportDir()
    {
        return Optional.ofNullable( this.consistencyReportLocation )
                .map( value -> format( "--cc-report-dir=%s", value ))
                .orElse( "" );
    }

    private String argCheckConsistency()
    {
        return Optional.ofNullable( this.checkConsistency )
                .map( value -> format( "--check-consistency=%s", value ) )
                .orElse( "" );
    }

    /**
     * Removes empty args and is a convenience method
     * @param args nullable, can be empty
     * @return cleaned command line parameters
     */
    private static String[] args( String... args )
    {
        return Stream.of( args )
                .filter( StringUtils::isNoneEmpty )
                .toArray( String[]::new );
    }

    private static Path configDirFromTarget( Path neo4jHome )
    {
        return neo4jHome.resolve( "conf" );
    }
}
