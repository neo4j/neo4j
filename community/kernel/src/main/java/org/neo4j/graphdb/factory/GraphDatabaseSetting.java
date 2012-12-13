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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Setting types for Neo4j. Actual settings are in GraphDatabaseSettings.
 *
 * This is a usage-only class, backwards compatibility is retained for using implementations
 * of it, but not for implementing it.
 *
 * This is deprecated, please use {@link Setting} instead.
 */
// Deprecated because we want to make this internal. Users should use Setting<T> instead.
@Deprecated
public abstract class GraphDatabaseSetting<T> implements Setting<T>
{
    // Deprecated because we want to move this out of the public API
    @Deprecated
    public static final String TRUE = "true";

    // Deprecated because we want to move this out of the public API
    @Deprecated
    public static final String FALSE = "false";

    // Deprecated because we want to move this out of the public API
    @Deprecated
    public static final String ANY = ".+";

    // Deprecated because this is to be removed
    @Deprecated
    public static final String SIZE = "\\d+[kmgKMG]";

    // Deprecated because this is to be removed
    @Deprecated
    public static final String DURATION = "\\d+(ms|s|m)";


    public interface DefaultValue
    {
        String getDefaultValue();
    }
    
    //
    // Implementations of GraphDatabaseSetting
    //

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class StringSetting
        extends GraphDatabaseSetting<String>
    {
        private Pattern regex;

        public StringSetting() 
        {
            this( "", ANY, "Must be a non-empty string." );
        }
        
        public StringSetting( String name, String regex, String formatMessage )
        {
            super( name, formatMessage );
            this.regex = Pattern.compile( regex );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );

            if (!regex.matcher( value ).matches())
            {
                throw illegalValue( locale, value, new String[0] );
            }
        }
        
        @Override
        public String valueOf(String rawValue, Config config)
        {
            return rawValue;
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static abstract class NumberSetting<T extends Number>
        extends GraphDatabaseSetting<T>
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
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
		protected void rangeCheck(Locale locale, Comparable value)
        {
            // Check range
            if (min != null && value.compareTo( min ) < 0)
                throw illegalValue(locale, value+"", "Minimum allowed value is: %s", new String[]{min+""} );

            if (max != null && value.compareTo( max ) > 0)
            	throw illegalValue(locale, value+"", "Maximum allowed value is: %s", new String[]{max+""} );
        }

        @Deprecated
        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected void rangeCheck(Comparable value)
        {
            rangeCheck( Locale.getDefault(), value );
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

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
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
                throw illegalValue( locale, value, new String[0] );

            int val;
            try
            {
                val = Integer.parseInt( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value, new String[0] );
            }

            rangeCheck(locale, val );
        }

        @Override
        public Integer valueOf(String rawValue, Config config) 
        {
            return Integer.valueOf(rawValue);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
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
                throw illegalValue( locale, value, new String[0] );

            long val;
            try
            {
                val = Long.parseLong( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value, new String[0] );
            }

            rangeCheck(locale, val );
        }
        
        @Override
        public Long valueOf(String rawValue, Config config) 
        {
            return Long.valueOf(rawValue);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
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
                throw illegalValue( locale, value, new String[0] );

            float val;
            try
            {
                val = Float.parseFloat( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value, new String[0] );
            }

            rangeCheck(locale, val );
        }
        
        @Override
        public Float valueOf(String rawValue, Config config) 
        {
            return Float.valueOf(rawValue);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
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
                throw illegalValue( locale, value, new String[0] );

            double val;
            try
            {
                val = Double.parseDouble( value );
            }
            catch( Exception e )
            {
                throw illegalValue( locale, value, new String[0] );
            }

            rangeCheck(locale, val );
        }
        
        @Override
        public Double valueOf(String rawValue, Config config) 
        {
            return Double.valueOf( rawValue );
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class PortSetting
        extends IntegerSetting
    {
        public PortSetting( String name )
        {
            super(name, "Must be a valid port number", 1, 65535);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class TimeSpanSetting extends GraphDatabaseSetting<Long>
    {

        // Regular expression that matches a duration e.g. 10ms or 5s
        private final Pattern timeSpanRegex = Pattern.compile("\\d+(ms|s|m)"); 
        
        public TimeSpanSetting( String name )
        {
            super(name, "Must be a valid time span");
        }
        
        @Override
        public void validate( Locale locale, String value )
            throws IllegalArgumentException
        {
            if(value == null)
                throw illegalValue( locale, value, new String[0] );
            
            if (!timeSpanRegex.matcher( value ).matches())
            {
                throw illegalValue( locale, value, new String[0] );
            }
        }
        
        @Override
        public Long valueOf(String rawValue, Config config) 
        {
            return TimeUtil.parseTimeMillis(rawValue);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static abstract class BaseOptionsSetting<ST>
        extends GraphDatabaseSetting<ST>
    {
        String[] options;
        
        protected BaseOptionsSetting( String name, String... options)
        {
            super( name, "Invalid option. Valid options are:%s");
            
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
            
            throw illegalValue( locale, value, new String[]{Arrays.asList( options() ).toString()} );
        }

        public String[] options()
        {
            return options;
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class OptionsSetting extends BaseOptionsSetting<String> {

        protected OptionsSetting(String name, String ... options)
        {
            super(name, options);
        }

        @Override
        public String valueOf(String rawValue, Config config)
        {
            return rawValue;
        }
        
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class EnumerableSetting<ET extends Enum<ET>> extends BaseOptionsSetting<ET> {

        private static String[] enumSetToStringArray(EnumSet<?> enums) {
            String [] stringValues = new String[enums.size()];
            int i=0;
            for(Enum<?> v : enums) 
                stringValues[i++] = v.name().toLowerCase();
            return stringValues;
        }

        private final Class<ET> backingEnum;
        
        public EnumerableSetting(String name, Class<ET> theEnum)
        {
            super(name, enumSetToStringArray(EnumSet.allOf(theEnum)));
            this.backingEnum = theEnum;
        }

        @Override
        public ET valueOf(String rawValue, Config config)
        {
            return Enum.valueOf(backingEnum, rawValue);
        }
        
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class BooleanSetting
        extends BaseOptionsSetting<Boolean>
    {
        public BooleanSetting( String name)
        {
            super( name, TRUE, FALSE );
        }
        
        @Override
        public Boolean valueOf(String rawValue, Config config) 
        {
            return Boolean.parseBoolean(rawValue);
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class AbstractPathSetting
    extends StringSetting
    {
        private DirectorySetting relativeTo;
        private boolean makeCanonical;
        private boolean fixIncorrectPathSeparators;
    
        public AbstractPathSetting( String name )
        {
            this( name, null, false, false);
        }
        
        /**
         * @param name
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public AbstractPathSetting( String name, boolean makeCanonical, boolean fixIncorrectPathSeparators)
        {
            this( name, null, makeCanonical, fixIncorrectPathSeparators);
        }
        
        /**
         * @param name
         * @param relativeTo If the configured value is a relative path, make it relative to this config setting.
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public AbstractPathSetting( String name, DirectorySetting relativeTo, boolean makeCanonical, boolean fixIncorrectPathSeparators) {
            super( name, ".*", "Must be a valid file path.");
            this.relativeTo = relativeTo;
            this.makeCanonical = makeCanonical;
            this.fixIncorrectPathSeparators = fixIncorrectPathSeparators;
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );
        }
        
        @Override
        public String valueOf(String rawValue, Config config) 
        {
            if(fixIncorrectPathSeparators) 
            {
                rawValue = FileUtils.fixSeparatorsInPath(rawValue);
            }
            
            File path = new File(rawValue);

            if(!path.isAbsolute() && relativeTo != null)
            {
                File baseDir = new File(config.get(relativeTo));
                path = new File(baseDir, rawValue);
            }

            if(makeCanonical)
            {
                try
                {
                    return path.getCanonicalPath();
                } catch (IOException e)
                {
                    if (path.isAbsolute())
                    {
                        return path.getAbsolutePath();
                    }

                    throw new IllegalArgumentException(name() + ": unable to resolve canonical path for " + rawValue + ".", e);
                }
            } else if( path.isAbsolute()) 
            {
                return path.getAbsolutePath();
            } else 
            {
                return rawValue;
            }
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class FileSetting
        extends AbstractPathSetting
    {
    
        public FileSetting( String name )
        {
            super( name, null, false, false);
        }
        
        /**
         * @param name
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public FileSetting( String name, boolean makeCanonical, boolean fixIncorrectPathSeparators)
        {
            super( name, null, makeCanonical, fixIncorrectPathSeparators);
        }
        
        /**
         * @param name
         * @param relativeTo If the configured value is a relative path, make it relative to this config setting.
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public FileSetting( String name, DirectorySetting relativeTo, boolean makeCanonical, boolean fixIncorrectPathSeparators) {
            super( name, relativeTo, makeCanonical, fixIncorrectPathSeparators);
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );
            
            File file = new File(value);
            if(file.exists() && !file.isFile())
                throw illegalValue( locale, value, new String[0] );
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class DirectorySetting
        extends AbstractPathSetting
    {
    
        public DirectorySetting( String name )
        {
            super( name, null, false, false);
        }
        
        /**
         * @param name
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public DirectorySetting( String name, boolean makeCanonical, boolean fixIncorrectPathSeparators)
        {
            super( name, null, makeCanonical, fixIncorrectPathSeparators);
        }
        
        /**
         * @param name
         * @param relativeTo If the configured value is a relative path, make it relative to this config setting.
         * @param makeCanonical Resolve symbolic links and clean up the path string before returning it.
         * @param fixIncorrectPathSeparators Ensure that path separators are correct for the current platform.
         */
        public DirectorySetting( String name, DirectorySetting relativeTo, boolean makeCanonical, boolean fixIncorrectPathSeparators) {
            super( name, relativeTo, makeCanonical, fixIncorrectPathSeparators );
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );
            
            File dir = new File(value);
            if(dir.exists() && !dir.isDirectory())
                throw illegalValue( locale, value, new String[0] );
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class NumberOfBytesSetting
        extends GraphDatabaseSetting<Long>
    {
        // Regular expression that matches a size e.g. 512M or 2G
        private final Pattern sizeRegex = Pattern.compile("\\d+ *[kmgKMG]?");
    
        public NumberOfBytesSetting( String name )
        {
            super( name, "%s is not a valid size, must be e.g. 10, 5K, 1M, 11G");
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );
            
            if(!sizeRegex.matcher(value).matches())
                throw illegalValue( locale, value, new String[0] );
        }
        
        @Override
        public Long valueOf(String rawValue, Config config) 
        {
            return parseNumberOfBytes( rawValue );
        }

        public static Long parseNumberOfBytes( String rawValue )
        {
            String mem = rawValue.toLowerCase();
            long multiplier = 1;
            if ( mem.endsWith( "k" ) )
            {
                multiplier = 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "m" ) )
            {
                multiplier = 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }

            return Long.parseLong( mem.trim() ) * multiplier;
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class IntegerRangeNumberOfBytesSetting extends GraphDatabaseSetting<Integer>
    {
        private final GraphDatabaseSetting<Long> fullRange;
        private final int atLeast;
        
        public IntegerRangeNumberOfBytesSetting( String name )
        {
            this( name, 0 );
        }
        
        public IntegerRangeNumberOfBytesSetting( String name, int atLeast )
        {
            super( name, "" );
            this.atLeast = atLeast;
            this.fullRange = new NumberOfBytesSetting( name );
        }

        @Override
        public void validate( Locale locale, String value )
        {
            fullRange.validate( locale, value );
            Long bytes = fullRange.valueOf( value, null );
            if ( bytes.longValue() > Integer.MAX_VALUE )
                throw illegalValue( locale, value, "Size too big, keep withing interger range (2^32-1)", "" + bytes );
            int result = bytes.intValue();
            if ( result < atLeast )
                throw illegalValue( locale, value, "Size too low, must be at least " + atLeast );
        }

        @Override
        public Integer valueOf( String rawValue, Config config )
        {
            return fullRange.valueOf( rawValue, config ).intValue();
        }
    }

    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class ListSetting<T>
        extends GraphDatabaseSetting<List<T>>
    {
        private GraphDatabaseSetting<T> itemSetting;
        private String separator;

        public ListSetting( String name, GraphDatabaseSetting<T> itemSetting )
        {
            this(name, itemSetting, ",");
        }
        
        public ListSetting( String name, GraphDatabaseSetting<T> itemSetting, String separator )
        {
            super( name, "%s is not a valid list, must be '"+separator+"' separated list of values.");
            this.itemSetting = itemSetting;
            this.separator = separator;
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if (value == null)
                throw illegalValue( locale, value, new String[0] );
            
            if( value.length() == 0)
                return;
            
            for(String item : value.split(separator) ) 
            {
                itemSetting.validate(item);
            }
        }
        
        @Override
        public List<T> valueOf(String rawValue, Config config) 
        {
            List<T> list = new ArrayList<T>();
            if(rawValue.length() > 0)
            {
                for(String item : rawValue.split(separator) ) 
                {
                    list.add(itemSetting.valueOf(item, config));
                }
            }
            return list;
        }
    }



    /**
     * This is deprecated, because it is going to be moved out of the public API. Please use {@link Setting} instead.
     */
    @Deprecated
    public static class URISetting extends GraphDatabaseSetting<URI>
    {
        private boolean normalize;
    
        public URISetting( String name )
        {
            this( name, false);
        }
        
        public URISetting( String name, boolean normalize) {
            super( name, "Value given does not validate as a proper URI.");
            this.normalize = normalize;
        }
    
        @Override
        public void validate( Locale locale, String value )
        {
            if(value == null)
                throw illegalValue(locale, null, new String[0]);
            
            try
            {
                new URI( value ).normalize();
            }
            catch ( URISyntaxException e )
            {
                throw illegalValue(locale, value, new String[0]);
            }
        }
        
        @Override
        public URI valueOf(String rawValue, Config config) 
        {
            URI uri = null;
            try
            {
                uri = new URI( rawValue );

                if(normalize)   
                {
                    String resultStr = uri.normalize().toString();
                    if ( resultStr.endsWith( "/" ) )
                    {
                        uri = new URI( resultStr.substring(0, resultStr.length() - 1));
                    }
                }
            }
            catch ( URISyntaxException e )
            {
                throw new RuntimeException("Unable to get URI value from config, see nested exception", e);
            }

            return uri;
        }
    }
    
    //
    // Actual class implementation
    //
    

    private final String name;
    private final String validationMessage;

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

    /**
     * Validate a raw string value, called when configuration is set.
     * Throws IllegalArgumentException if the provided value is not valid.
     * 
     * @param locale
     * @param value
     */
    public abstract void validate( Locale locale, String value );
    
    /**
     * Create a typed value from a raw string value. This is to be called
     * when a value is fetched from configuration.
     * 
     * @param rawValue The raw string value stored in configuration
     * @param config The config instance, allows having config values that depend on each other.
     * @return
     */
    public abstract T valueOf(String rawValue, Config config);


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

    @Deprecated
    protected IllegalArgumentException illegalValue(Locale locale, String... args)
    {
        return illegalValue( locale, "<unknown>" , args);
    }

    protected IllegalArgumentException illegalValue(Locale locale, String value, Object [] args)
        throws IllegalArgumentException
    {
        return illegalValue(locale, value, validationMessage, args);
    }
    
    protected IllegalArgumentException illegalValue(Locale locale, String value, String rawMessage, Object [] args)
            throws IllegalArgumentException
        {
            String message = getMessage( locale, rawMessage );
            String errorMessage = new Formatter(locale).format( message, args ).toString();
            
            String settingNameMessage = getMessage( locale, "Invalid value %s for config property '%s': " );
            String settingMessage = new Formatter(locale).format( settingNameMessage, value == null?"[null]":"'"+value+"'", name() ).toString();
            
            return new IllegalArgumentException( settingMessage + errorMessage );
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