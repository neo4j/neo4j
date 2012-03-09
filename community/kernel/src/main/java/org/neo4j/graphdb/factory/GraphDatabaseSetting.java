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
package org.neo4j.graphdb.factory;

import java.util.Arrays;
import java.util.Locale;

/**
 * Settings for the Community edition of Neo4j. Use this with GraphDatabaseBuilder.
 */
public abstract class GraphDatabaseSetting
{
    @Default(BooleanSetting.FALSE)
    public static final BooleanSetting read_only = new BooleanSetting("read_only");

    // Implementations of GraphDatabaseSetting
    public static class BooleanSetting
        extends OptionsSetting
    {
        public static final String TRUE = "true";
        public static final String FALSE = "false";

        public BooleanSetting( String name)
        {
            super( name );
        }

        public String[] options()
        {
            return new String[]{TRUE, FALSE};
        }
    }

    public static abstract class OptionsSetting
        extends GraphDatabaseSetting
    {
        protected OptionsSetting( String name)
        {
            super( name );
        }

        @Override
        public void validate( String value, Locale locale )
            throws IllegalArgumentException
        {
            for( String option : options() )
            {
                if (value.equalsIgnoreCase( option ))
                    return;
            }

            throw new IllegalArgumentException( "Value '"+value+"' is not valid. Valid options are:"+ Arrays.asList( options() ) );
        }

        public abstract String[] options();
    }

    private String name;

    protected GraphDatabaseSetting( String name )
    {
        this.name = name;
    }

    public String name()
    {
        return name;
    }
    
    public String defaultValue()
    {
        try
        {
            return GraphDatabaseSetting.class.getField( name ).getAnnotation( Default.class ).value();
        }
        catch( NoSuchFieldException e )
        {
            throw new NoSuchFieldError( "Setting field "+name+" not found in GraphDatabaseSetting class" );
        }
    }
    
    public void validate(String value)
        throws IllegalArgumentException
    {
        validate( value, Locale.getDefault() );
    }
    
    public abstract void validate(String value, Locale locale);
}
