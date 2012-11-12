/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;

import org.neo4j.graphdb.factory.SettingsResourceBundle;

/**
 * Generates Asciidoc by using subclasses of {@link org.neo4j.graphdb.factory.SettingsResourceBundle},
 * to pick up localized documentation of the documentation.
 * 
 * Format
 * <pre>
 * .Title
 * [configsetting]
 * ----
 * key: default_value
 * description
 * value1: (description1)
 * value2: (description2)
 * ----
 * </pre>
 */
public class ConfigAsciiDocGenerator {

	public String generateDocsFor(Class<? extends SettingsResourceBundle> settingsResource)
	{
		return generateDocsFor(settingsResource.getName());
	}
	
	public String generateDocsFor(String settingsResource)
	{
		ResourceBundle bundle = ResourceBundle.getBundle( settingsResource );
		
		StringBuilder sb = new StringBuilder();
		
		List<String> keys = new ArrayList<String>(bundle.keySet());
		Collections.sort(keys);
		
        for( String property : keys )
        {
            if (property.endsWith( ".description" ))
            {
                String name = property.substring( 0, property.lastIndexOf( "." ) );
                sb.append("."+bundle.getString( name+".title" )+"\n");
                
                String minmax = "";
                if (bundle.containsKey( name+".min" ) && bundle.containsKey( name+".max" ))
                    minmax=",\"minmax\"";
                else if (bundle.containsKey( name+".min" ))
                    minmax=",\"min\"";
                else if (bundle.containsKey( name+".max" ))
                    minmax=",\"max\"";

                sb.append( "[\"configsetting\""+minmax+"]\n");
                sb.append( "----\n" );

                String defaultKey = name + ".default";
                if (bundle.containsKey( defaultKey ))
                {
                	sb.append( name+": "+bundle.getString( defaultKey )+"\n");
                } else
                {
                	sb.append( name+"\n");
                }

                sb.append( bundle.getString( property )+"\n");
                
                // Output optional options
                String optionsKey = name+".options";
                if (bundle.containsKey( optionsKey ))
                {
                    String[] options = bundle.getString( optionsKey ).split( "," );
                    if (bundle.containsKey( name+".option."+options[0] ))
                    {
                        for( String option : options )
                        {
                            String description = bundle.getString( name + ".option." + option );
                            char[] spaces = new char[ option.length() + 2 ];
                            Arrays.fill( spaces,' ' );
                            description = description.replace( "\n", " ");
                            sb.append(option+": "+ description+"\n");
                        }
                    } else
                    {
                    	sb.append(bundle.getString( optionsKey ).replace( ","," \n" )+"\n");
                    }
                }

                if (bundle.containsKey( name+".min" ))
                	sb.append(bundle.getString( name+".min" )+"\n");
                if (bundle.containsKey( name+".max" ))
                	sb.append(bundle.getString( name+".max" )+"\n");

                sb.append( "----\n" );
                sb.append( "\n" );
            }
        }
        
        return sb.toString();
	}
	
}
