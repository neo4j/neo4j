/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.neo4j.rest.domain.Representation;
import org.neo4j.server.webadmin.domain.NoSuchPropertyException;
import org.neo4j.server.webadmin.domain.ServerPropertyRepresentation;

/**
 * This class contains contains the code for loading and saving server
 * properties. This includes reading and writing properties to and from the
 * appropriate configuration files.
 * 
 * TODO: This was a bad idea from the beginning. Since then, it has far outgrown
 * its initial purpose and is in heavy need of refactoring.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ServerConfiguration implements Representation
{
//
//    public static final String FALLBACK_MAX_HEAP = "512M";
//    public static final String FALLBACK_MIN_HEAP = "512M";
//
    private static ServerConfiguration INSTANCE;

    /**
     * This is the properties file that is read and used by the underlying neo4j
     * instance.
     */
    private Properties dbConfig;

    /**
     * This property file stores configuration used when launching the JVM and
     * when creating a new database.
     */
    private Properties generalConfig;

    private ArrayList<ServerPropertyRepresentation> properties = ServerPropertyDefinitions.getPropertyDefinitions();

    /**
     * Singleton instance.
     * 
     * @return
     * @throws IOException
     */
    public static ServerConfiguration getInstance()
    {
        if ( INSTANCE == null )
        {
            INSTANCE = new ServerConfiguration();
        }
        return INSTANCE;
    }
//
//    //
//    // CONSTRUCT
//    //
//
//    protected ServerConfiguration()
//    {
//        try
//        {
//            if ( dbConfig == null )
//            {
//                dbConfig = new Properties();
//
//                if ( DatabaseLocator.isLocalDatabase() )
//                {
//                    FileInputStream in = new FileInputStream(
//                            ConfigFileFactory.getDbConfigFile() );
//                    dbConfig.load( in );
//                    in.close();
//                }
//            }
//
//            if ( generalConfig == null )
//            {
//                generalConfig = new Properties();
//                FileInputStream in = new FileInputStream(
//                        ConfigFileFactory.getGeneralConfigFile() );
//                generalConfig.load( in );
//                in.close();
//            }
//        }
//        catch ( IOException e )
//        {
//            throw new RuntimeException( "Unable to load property files.", e );
//        }
//
//        // Go through properties and set them to their current values
//        for ( ServerPropertyRepresentation prop : properties )
//        {
//            switch ( prop.getType() )
//            {
//            case CONFIG_PROPERTY:
//                if ( dbConfig.containsKey( prop.getKey() ) )
//                {
//                    prop.setFullValue( dbConfig.getProperty( prop.getKey() ) );
//                }
//            default:
//                if ( generalConfig.containsKey( prop.getKey() ) )
//                {
//                    prop.setFullValue( generalConfig.getProperty( prop.getKey() ) );
//                }
//            }
//        }
//    }
//
//    //
//    // PUBLIC
//    //

    public Object serialize()
    {
        ArrayList<Object> serial = new ArrayList<Object>();
//        for ( ServerPropertyRepresentation prop : properties )
//        {
//            serial.add( prop.serialize() );
//        }
        return serial;
    }
//
//    /**
//     * Change a setting. This will write to configuration files, but does not
//     * restart any running server instance.
//     * 
//     * @param key
//     * @param value
//     * @throws IOException if writing changes to disk fails
//     * @throws NoSuchPropertyException if no property with the given key exists.
//     * @throws IllegalArgumentException if trying to set an invalid value
//     */
//    public synchronized void set( String key, String value ) throws IOException
//    {
//        ServerPropertyRepresentation prop = this.get( key );
//
//        if ( !prop.isValidValue( value ) )
//        {
//            throw new IllegalArgumentException(
//                    "'" + value + "' is not a valid value for property '" + key
//                            + "'." );
//        }
//
//        prop.setValue( value );
//
//        switch ( prop.getType() )
//        {
//        case CONFIG_PROPERTY:
//            if ( DatabaseLocator.isLocalDatabase() )
//            {
//                dbConfig.put( key, prop.getFullValue() );
//                saveProperties( dbConfig, ConfigFileFactory.getDbConfigFile() );
//            }
//            break;
//        default:
//            generalConfig.put( key, prop.getFullValue() );
//            saveProperties( generalConfig,
//                    ConfigFileFactory.getGeneralConfigFile() );
//            writeJvmAndAppArgs();
//        }
//    }
//
    /**
     * Get a property by key.
     * 
     * @param key
     * @return
     * @throws NoSuchPropertyException if no property with the given key exists.
     */
    public ServerPropertyRepresentation get( String key )
    {
        for ( ServerPropertyRepresentation prop : properties )
        {
            if ( prop.getKey().equals( key ) )
            {
                return prop;
            }
        }

        throw new NoSuchPropertyException( "Property with key '" + key
                                           + "' does not exist." );
    }
//
//    //
//    // INTERNALS
//    //
//
//    protected void saveProperties( Properties prop, File file )
//            throws IOException
//    {
//        FileOutputStream out = new FileOutputStream( file );
//        prop.store( out, "--Changed via admin gui--" );
//        out.close();
//    }
//
//    /**
//     * Update the file with extra args passed to the JVM and to the application
//     * itself to match the currently set startup args.
//     * 
//     * @throws IOException
//     * @throws FileNotFoundException
//     */
//    protected synchronized void writeJvmAndAppArgs()
//            throws FileNotFoundException, IOException
//    {
//        File serviceConfig = ConfigFileFactory.getServiceConfigFile();
//
//        if ( serviceConfig != null )
//        {
//            // PRODUCTION MODE
//
//            // So uh. This is not very nice.
//            // The reason for this is to a) conserve config file formatting and
//            // b) config file is not a "normal" properties file, and escaping
//            // special
//            // chars etc. that is done by Properties will screw up running the
//            // service. So we have to do some manual labor here.
//
//            // This will contain the end-result config file
//            StringBuilder configFileBuilder = new StringBuilder();
//
//            // Read the whole config file, discard any pre-existing JVM args
//            FileReader in = new FileReader( serviceConfig );
//            BufferedReader br = new BufferedReader( in );
//            String line;
//            while ( ( line = br.readLine() ) != null )
//            {
//                if ( !line.startsWith( "wrapper.java.additional" ) )
//                {
//                    configFileBuilder.append( line );
//                    configFileBuilder.append( "\n" );
//                }
//            }
//
//            in.close();
//
//            // JVM Args
//            configFileBuilder.append( propertiesToJSWConfigString(
//                    "wrapper.java.additional.",
//                    ServerPropertyRepresentation.PropertyType.JVM_ARGUMENT, 1 ) );
//
//            // App args
//            configFileBuilder.append( "wrapper.app.parameter.1=org.neo4j.server.webadmin.Main\n" );
//            configFileBuilder.append( propertiesToJSWConfigString(
//                    "wrapper.app.parameter.",
//                    ServerPropertyRepresentation.PropertyType.APP_ARGUMENT, 2 ) );
//
//            // Write changes to file.
//            FileOutputStream out = new FileOutputStream( serviceConfig );
//            out.write( configFileBuilder.toString().getBytes() );
//            out.close();
//        }
//        else
//        {
//            // DEVELOPMENT MODE
//
//            // Write jvm args
//
//            FileOutputStream jvmArgs = new FileOutputStream(
//                    ConfigFileFactory.getDevelopmentJvmArgsFile() );
//            jvmArgs.write( propertiesToSpaceSeparatedString(
//                    ServerPropertyRepresentation.PropertyType.JVM_ARGUMENT ).getBytes() );
//            jvmArgs.close();
//
//            // Write app args
//
//            FileOutputStream appArgs = new FileOutputStream(
//                    ConfigFileFactory.getDevelopmentAppArgsFile() );
//            appArgs.write( propertiesToSpaceSeparatedString(
//                    ServerPropertyRepresentation.PropertyType.APP_ARGUMENT ).getBytes() );
//            appArgs.close();
//
//        }
//    }
//
//    /**
//     * Write a string with space-separated properties of a given type.
//     * 
//     * @param type
//     * @return
//     */
//    private String propertiesToSpaceSeparatedString(
//            ServerPropertyRepresentation.PropertyType type )
//    {
//        StringBuilder args = new StringBuilder();
//
//        for ( ServerPropertyRepresentation prop : properties )
//        {
//            if ( prop.getType() == type )
//            {
//                args.append( prop.getFullValue() );
//                args.append( " " );
//            }
//        }
//
//        String out = args.toString();
//        return out.length() > 0 ? out.substring( 0, out.length() - 1 ) : "";
//    }
//
//    private String propertiesToJSWConfigString( String prepend,
//            ServerPropertyRepresentation.PropertyType type, int argNo )
//    {
//        StringBuilder builder = new StringBuilder();
//
//        for ( ServerPropertyRepresentation prop : properties )
//        {
//            if ( prop.getType() == type )
//            {
//                builder.append( prepend );
//                builder.append( ( argNo++ ) );
//                builder.append( "=" );
//                builder.append( prop.getFullValue() );
//                builder.append( "\n" );
//            }
//        }
//
//        return builder.toString();
//    }
}
