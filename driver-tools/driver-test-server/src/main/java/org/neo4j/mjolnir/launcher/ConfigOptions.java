/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.mjolnir.launcher;

import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Internal;

import static org.neo4j.helpers.Strings.ljust;

public class ConfigOptions
{
    private final Class[] settingsClasses;

    public ConfigOptions()
    {
        this( HandAssembledNeo4j.Settings.class,
              GraphDatabaseSettings.class,
              UdcSettings.class );
    }

    public ConfigOptions( Class ... settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    public void describeTo( PrintStream out )
    {
        List<DescribableSetting> descriptions = new ArrayList<>();
        int keyColumnWidth = 0;
        for ( Class settingsClass : settingsClasses )
        {
            for ( Field field : settingsClass.getDeclaredFields() )
            {
                try
                {
                    if ( Setting.class.isAssignableFrom( field.getType() )
                         && !(field.isAnnotationPresent( Deprecated.class ))
                         && !(field.isAnnotationPresent( Internal.class )) )
                    {
                        Setting setting = (Setting) field.get( null );
                        keyColumnWidth = Math.max( keyColumnWidth, setting.name().length() );

                        descriptions.add( descriptionOf( field, setting ) );
                    }
                } catch(Throwable e)
                {
                    // Ignore fields we can't access.
                }
            }
        }

        Collections.sort( descriptions, new Comparator<DescribableSetting>()
        {
            @Override
            public int compare( DescribableSetting o1, DescribableSetting o2 )
            {
                return o1.compareTo( o2 );
            }
        } );

        for ( DescribableSetting description : descriptions )
        {
            out.println( description.describe( keyColumnWidth + 2 ) );
        }
    }

    private DescribableSetting descriptionOf( Field field, Setting setting )
    {
        String description = "N/A";

        for ( Annotation annotation : field.getDeclaredAnnotations() )
        {
            if(annotation instanceof Description )
            {
                description = ((Description)annotation).value();
            }
        }

        return new DescribableSetting( setting.name(), description, setting.getDefaultValue() );
    }

    private static class DescribableSetting implements Comparable<DescribableSetting>
    {
        private final String name;
        private final String description;
        private final String defaultValue;

        public DescribableSetting( String name, String description, String defaultValue )
        {
            this.name = name;
            this.description = description;
            this.defaultValue = defaultValue;
        }

        @Override
        public int compareTo( DescribableSetting o )
        {
            return o.name.compareTo( name );
        }

        public String describe( int keyColumnWidth )
        {
            StringBuilder sb = new StringBuilder();

            sb.append( ljust( name, keyColumnWidth ) )
              .append( description );

            if(defaultValue != null)
            {
                sb.append( " [default: " ).append( defaultValue ).append( "]" );
            }
            else
            {
                sb.append( " [no default]" );
            }

            return sb.toString();
        }
    }
}
