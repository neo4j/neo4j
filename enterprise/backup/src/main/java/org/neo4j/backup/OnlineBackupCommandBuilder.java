/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
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
    private File  consistencyReportLocation;
    private Config additionalConfig;
    private Boolean consistencyCheckGraph;
    private Boolean consistencyCheckIndexes;
    private Boolean consistencyCheckLabel;
    private Boolean consistencyCheckOwners;
    private OutputStream output;

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

    /**
     * Perform a backup with the parameters set using the builder
     * @param targetLocation target location of backup (NOTE: uses the parent directory as well)
     * @return true if backup was successful
     */
    public boolean backup( String targetLocation ) throws CommandFailed, IncorrectUsage
    {
        return backup( relativeFileFromString( targetLocation ) );
    }

    /**
     * Perform a backup with the parameters set using the builder
     * @param targetLocation target location of backup (NOTE: uses the parent directory as well)
     * @return true if backup was successful
     */
    public boolean backup( File targetLocation ) throws CommandFailed, IncorrectUsage
    {
        new OnlineBackupCommandProvider()
                .create( neo4jHomeFromTarget( targetLocation ),
                        configDirFromTarget( targetLocation ),
                        resolveOutsideWorld() )
                .execute( resolveArgs( targetLocation ) );
        return true;
    }

    private File relativeFileFromString( String targetLocation )
    {
        return new File( "." ).toPath().resolve( targetLocation ).toFile();
    }

    /**
     * This is a helper method when debugging tests where in some cases the file is a relative string
     * @param targetLocation relative backup location as a string
     * @return the arguments that would be passed to the backup command
     */
    public String[] resolveArgs( String targetLocation )
    {
        return resolveArgs( relativeFileFromString( targetLocation ) );
    }

    public String[] resolveArgs( File targetLocation )
    {
        return args(
                argBackupName( targetLocation ),
                argBackupLocation( targetLocation ),
                argFrom(),
                argFallbackToFull(),
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
        return new ParametrisedOutsideWorld( System.console(), output.orElse( System.out ), output.orElse( System.err ), new DefaultFileSystemAbstraction() );
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
        return String.format( "--from=%s", address );
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
        return String.format( "--backup-dir=%s", location );
    }

    private String argBackupName( File targetLocation )
    {
        String backupName = Optional.ofNullable( targetLocation )
                .map( File::getName )
                .orElseThrow( wrongArguments( "No target location specified" ) );
        return String.format( "--name=%s", backupName );
    }

    private static Supplier<IllegalArgumentException> wrongArguments( String message )
    {
        return () -> new IllegalArgumentException( message );
    }

    private String argFallbackToFull()
    {
        return Optional.ofNullable( fallbackToFull )
                .map( flag -> String.format( "--fallback-to-full=%s", flag ) )
                .orElse( "" );
    }

    private String argTimeout()
    {
        return Optional.ofNullable( this.timeout )
                .map( value -> String.format("--timeout=%dms", value) )
                .orElse( "" );
    }

    private String argCcOwners()
    {
        return Optional.ofNullable( this.consistencyCheckOwners )
                .map( value -> String.format( "--check-consistency=%b", this.consistencyCheckOwners ) )
                .orElse( "" );
    }

    private String argCcLabel()
    {
        return Optional.ofNullable( this.consistencyCheckLabel )
                .map( value -> String.format( "--cc-label-scan-store=%b", this.consistencyCheckLabel ) )
                .orElse( "" );
    }

    private String argCcIndexes()
    {
        return Optional.ofNullable( this.consistencyCheckIndexes )
                .map( value -> String.format( "--cc-indexes=%b", this.consistencyCheckIndexes ) )
                .orElse( "" );
    }

    private String argCcGraph()
    {
        return Optional.ofNullable( this.consistencyCheckGraph )
                .map( value -> String.format( "--cc-graph=%b", this.consistencyCheckGraph ) )
                .orElse( "" );
    }

    private String argAdditionalConf( File backupTarget )
    {
        if ( additionalConfig == null )
        {
            return "";
        }
        File configFile = neo4jHomeFromTarget( backupTarget ).resolve( "../additional_neo4j.conf" ).toFile();
        writeConfigToFile( additionalConfig, configFile );

        return String.format( "--additional-config=%s", configFile );
    }

    private void writeConfigToFile( Config config, File file )
    {
        try
        {
            Writer fileWriter = new BufferedWriter( new FileWriter( file ) );
            for ( Map.Entry<String,String> entry : config.getRaw().entrySet() )
            {
                fileWriter.write( String.format( "%s=%s\n", entry.getKey(), entry.getValue() ) );
            }
            fileWriter.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private String argReportDir()
    {
        return Optional.ofNullable( this.consistencyReportLocation )
                .map( value -> String.format( "--cc-report-dir=%s", value ))
                .orElse( "" );
    }

    private String argCheckConsistency()
    {
        return Optional.ofNullable( this.checkConsistency )
                .map( value -> String.format( "--check-consistency=%s", value ) )
                .orElse( "" );
    }

    /**
     * Removes empty args and is a convenience method
     * @param args nullable, can be empty
     * @return cleaned command line parameters
     */
    private static String[] args( String... args )
    {
        List<String> cleanedArgs = Stream.of( args )
                .filter( Objects::nonNull )
                .filter( not( String::isEmpty ) )
                .collect( Collectors.toList() );
        String[] returnArgs = new String[cleanedArgs.size()];
        cleanedArgs.toArray( returnArgs );
        return returnArgs;
    }

    private static <E> Predicate<E> not( Predicate<E> predicate )
    {
        return item -> !predicate.test( item );
    }

    private static Path neo4jHomeFromTarget( File target )
    {
        return target.toPath();
    }

    private static Path configDirFromTarget( File target )
    {
        return neo4jHomeFromTarget( target ).resolve( "config" );
    }
}
