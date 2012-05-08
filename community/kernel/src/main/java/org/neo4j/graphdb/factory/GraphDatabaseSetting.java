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
import java.util.Formatter;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.regex.Pattern;

/**
 * Setting types for Neo4j. Actual settings are in GraphDatabaseSettings
 */
public abstract class GraphDatabaseSetting
{
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    
    // Regular expression that matches any non-empty string
    public static final String ANY = ".+";
    
    // Regular expression that matches a size e.g. 512M or 2G
    public static final String SIZE = "\\d+[kmgKMG]";

    // Regular expression that matches a duration e.g. 10ms or 5s
    public static final String DURATION = "\\d+(ms|s|m)";

    public interface DefaultValue
    {
        String getDefaultValue();
    }
    
    // Implementations of GraphDatabaseSetting
    public static class BooleanSetting
        extends OptionsSetting
    {
        public BooleanSetting( String name)
        {
            super( name, TRUE, FALSE );
        }
    }
    
    public static class StringSetting
        extends GraphDatabaseSetting
    {
        private Pattern regex;

        public StringSetting( String name, String regex, String formatMessage )
        {
            super( name, formatMessage );
            this.regex = Pattern.compile( regex );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value );

            if (!regex.matcher( value ).matches())
            {
                throw illegalValue( locale, value );
            }
        }
    }
    
    public static abstract class NumberSetting<T extends Number>
        extends GraphDatabaseSetting
    {
        protected T min;
        protected T max;

        protected NumberSetting( String name, String validationMessage )
        {
            super( name, validationMessage );
        }

        protected NumberSetting( String name, String validationMessage, T min, T max )
        {
            super( name, validationMessage );
            this.min = min;
            this.max = max;
        }
        
        protected void rangeCheck(Comparable value)
        {
            // Check range
            if (min != null && value.compareTo( min ) < 0)
                throw new IllegalArgumentException( "Minimum allowed value is:"+min );

            if (max != null && value.compareTo( max ) > 0)
                throw new IllegalArgumentException( "Maximum allowed value is:"+max );
        }

        public T getMin()
        {
            return min;
        }

        public T getMax()
        {
            return max;
        }
    }
    
    public static class IntegerSetting
        extends NumberSetting<Integer>
    {
        public IntegerSetting( String name, String formatMessage )
        {
            super( name, formatMessage );
        }

        public IntegerSetting( String name, String formatMessage, Integer min, Integer max )
        {
            super( name, formatMessage, min, max );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value );

            int val;
            try
            {
                val = Integer.parseInt( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value );
            }

            rangeCheck( val );
        }
    }

    public static class LongSetting
        extends NumberSetting<Long>
    {
        public LongSetting( String name, String formatMessage )
        {
            super( name, formatMessage );
        }

        public LongSetting( String name, String formatMessage, Long min, Long max )
        {
            super( name, formatMessage, min, max );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value );

            long val;
            try
            {
                val = Long.parseLong( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value );
            }

            rangeCheck( val );
        }
    }

    public static class FloatSetting
        extends NumberSetting<Float>
    {
        public FloatSetting( String name, String formatMessage )
        {
            super( name, formatMessage );
        }

        public FloatSetting( String name, String formatMessage, Float min, Float max )
        {
            super( name, formatMessage, min, max);
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value );

            float val;
            try
            {
                val = Float.parseFloat( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value );
            }

            rangeCheck( val );
        }
    }

    public static class DoubleSetting
        extends NumberSetting<Double>
    {
        public DoubleSetting( String name, String formatMessage )
        {
            super( name, formatMessage );
        }

        public DoubleSetting( String name, String formatMessage, Double min, Double max )
        {
            super( name, formatMessage, min, max );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value );

            double val;
            try
            {
                val = Double.parseDouble( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value );
            }

            rangeCheck( val );
        }
    }

    public static class PortSetting
        extends IntegerSetting
    {
        public PortSetting( String name )
        {
            super(name, "Must be a valid port number", 1, 65535);
        }
    }

    public static class OptionsSetting
        extends GraphDatabaseSetting
    {
        String[] options;
        
        protected OptionsSetting( String name, String... options)
        {
            super( name, "Value '%s' is not valid. Valid options are:%s");
            
            this.options  = options;
        }

        @Override
        public void validate( Locale locale, String value )
            throws IllegalArgumentException
        {
            for( String option : options() )
            {
                if (option.equalsIgnoreCase( value ))
                    return;
            }
            
            throw illegalValue( locale, value, Arrays.asList( options() ).toString() );
        }


        public String[] options()
        {
            return options;
        }
    }

    private String name;
    private String validationMessage;

    protected GraphDatabaseSetting( String name, String validationMessage )
    {
        this.name = name;
        this.validationMessage = validationMessage;
    }

    public String name()
    {
        return name;
    }
    
    public String validationMessage()
    {
        return validationMessage;
    }
    
    public void validate(String value)
        throws IllegalArgumentException
    {
        validate( Locale.getDefault(), value );
    }
    
    public abstract void validate( Locale locale, String value );
    
    protected String getMessage(Locale locale, String defaultMessage)
    {
        if (locale.getLanguage().equals( Locale.ENGLISH.getLanguage() ))
            return defaultMessage;

        try
        {
            ResourceBundle bundle = ResourceBundle.getBundle( getClass().getName() );
            return bundle.getString( name() );
        }
        catch( Exception e )
        {
            return defaultMessage;
        }
    }

    protected IllegalArgumentException illegalValue(Locale locale, String... args)
        throws IllegalArgumentException
    {
        String message = getMessage( locale, validationMessage );
        Formatter formatter = new Formatter(locale);
        formatter.format( message, args );
        return new IllegalArgumentException( formatter.toString() );
    }

    public static boolean osIsWindows()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.startsWith("Windows");
    }

    public static boolean osIsMacOS()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.equalsIgnoreCase( "Mac OS X" );
    }
}
