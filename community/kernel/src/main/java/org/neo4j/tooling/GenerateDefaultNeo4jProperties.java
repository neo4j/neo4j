/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tooling;

import java.util.Arrays;
import java.util.ResourceBundle;
import java.util.Set;
import org.neo4j.graphdb.factory.GraphDatabaseSettingsResourceBundle;
import org.neo4j.graphdb.factory.SettingsResourceBundle;

/**
 * Generates the default neo4j.properties file by using the {@link GraphDatabaseSettingsResourceBundle}
 */
public class GenerateDefaultNeo4jProperties
{
    public static void main( String[] args )
        throws ClassNotFoundException
    {
        for( String settingsClassName : args )
        {
            Class settingsClass = GenerateDefaultNeo4jProperties.class.getClassLoader().loadClass( settingsClassName );

            ResourceBundle bundle = new SettingsResourceBundle(settingsClass);

            if (bundle.containsKey( "description" ))
            {
                System.out.println( "# " );
                System.out.println( "# "+bundle.getString( "description" ) );
                System.out.println( "# " );
                System.out.println( );
            }

            Set<String> keys = bundle.keySet();
            for( String property : keys )
            {
                if (property.endsWith( ".description" ))
                {
                    // Output description
                    String name = property.substring( 0, property.lastIndexOf( "." ) );
                    System.out.println( "# "+bundle.getString( property ) );

                    // Output optional options
                    String optionsKey = name+".options";
                    if (bundle.containsKey( optionsKey ))
                    {
                        String[] options = bundle.getString( optionsKey ).split( "," );
                        if (bundle.containsKey( name+".option."+options[0] ))
                        {
                            System.out.println("# Valid settings:");
                            for( String option : options )
                            {
                                String description = bundle.getString( name + ".option." + option );
                                char[] spaces = new char[ option.length() + 3 ];
                                Arrays.fill( spaces,' ' );
                                description = description.replace( "\n", "\n#"+ new String( spaces ) );
                                System.out.println("# "+option+": "+ description );
                            }
                        } else
                        {
                            System.out.println("# Valid settings:"+bundle.getString( optionsKey ));
                        }
                    }

                    String defaultKey = name + ".default";
                    if (bundle.containsKey( defaultKey ))
                    {
                        System.out.println( name+"="+bundle.getString( defaultKey ) );
                    } else
                    {
                        System.out.println( "# "+name+"=" );
                    }
                    System.out.println( );
                }
            }
        }
    }
}
