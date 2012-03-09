package org.neo4j.graphdb.factory;

import java.util.Arrays;

/**
 * Settings for the Community edition of Neo4j. Use this with GraphDatabaseBuilder.
 */
public abstract class GraphDatabaseSetting
{
    public static final BooleanSetting read_only = new BooleanSetting(BooleanSetting.FALSE);

    // Implementations of GraphDatabaseSetting
    public static class BooleanSetting
        extends OptionsSetting
    {
        public static final String TRUE = "true";
        public static final String FALSE = "false";

        public BooleanSetting( String defaultValue )
        {
            super( defaultValue );
        }

        public String[] options()
        {
            return new String[]{TRUE, FALSE};
        }
    }

    public static abstract class OptionsSetting
        extends GraphDatabaseSetting
    {
        protected OptionsSetting( String defaultValue )
        {
            super( defaultValue );
        }

        @Override
        public void validate( String value )
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

    private String defaultValue;

    protected GraphDatabaseSetting( String defaultValue )
    {
        this.defaultValue = defaultValue;
    }

    public String name()
    {
        return getClass().getSimpleName();
    }
    
    public String defaultValue()
    {
        return defaultValue;
    }

    public abstract void validate(String value)
        throws IllegalArgumentException;
}
