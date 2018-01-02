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
package org.neo4j.kernel.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.logging.Log;

/**
 * A basic approach to implementing configuration migrations.
 * This applies migrations in both directions, meaning that you
 * can still continue to both read and write with the old configuration
 * value.
 */
public class BaseConfigurationMigrator implements ConfigurationMigrator
{
    public interface Migration
    {
        boolean appliesTo( Map<String,String> rawConfiguration );

        Map<String,String> apply( Map<String,String> rawConfiguration );

        String getDeprecationMessage();
    }

    /**
     * Base class for implementing a migration that applies to a specific config property key.
     *
     * By default, this class will print a  deprecation message and run {@link #setValueWithOldSetting(String, Map)}
     * if the specified property key has been set by the user. Override {@link #appliesTo(Map)} if you want to
     * trigger on more specific reasons than that.
     */
    public static abstract class SpecificPropertyMigration implements Migration
    {
        private final String propertyKey;
        private final String deprecationMessage;

        public SpecificPropertyMigration( String propertyKey, String deprecationMessage )
        {
            this.propertyKey = propertyKey;
            this.deprecationMessage = deprecationMessage;
        }

        @Override
        public boolean appliesTo( Map<String,String> rawConfiguration )
        {
            return rawConfiguration.containsKey( propertyKey );
        }

        @Override
        public Map<String,String> apply( Map<String,String> rawConfiguration )
        {
            String value = rawConfiguration.get( propertyKey );
            rawConfiguration.remove( propertyKey );
            setValueWithOldSetting( value, rawConfiguration );
            return rawConfiguration;
        }

        @Override
        public String getDeprecationMessage()
        {
            return deprecationMessage;
        }

        public abstract void setValueWithOldSetting( String value, Map<String,String> rawConfiguration );
    }

    public static class PropertyRenamed extends SpecificPropertyMigration
    {
        private final String newKey;

        public PropertyRenamed( String oldKey, String newKey, String deprecationMessage )
        {
            super( oldKey, deprecationMessage );
            this.newKey = newKey;
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            rawConfiguration.put( newKey, value );
        }
    }

    /**
     * A simple migration where a config value (usually an enum) has been renamed.
     * Eg. it used to be `dbms.thing=doit` and now it's `dbm.thing=gogogo`.
     */
    public static class ConfigValueChanged implements Migration
    {
        private final String propertyKey;
        private final String oldValue;
        private final String newValue;
        private final String message;

        public ConfigValueChanged( String propertyKey, String oldValue, String newValue, String message )
        {
            this.propertyKey = propertyKey;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.message = message;
        }

        @Override
        public boolean appliesTo( Map<String,String> rawConfiguration )
        {
            return rawConfiguration.containsKey( propertyKey )
                    && rawConfiguration.get( propertyKey ).equalsIgnoreCase( oldValue );
        }

        @Override
        public Map<String,String> apply( Map<String,String> rawConfiguration )
        {
            rawConfiguration.put( propertyKey, newValue );
            return rawConfiguration;
        }

        @Override
        public String getDeprecationMessage()
        {
            return message;
        }
    }

    public static Migration valueChanged( String key, String oldValue, String newValue, String deprecationMessage )
    {
        return new ConfigValueChanged( key, oldValue, newValue, deprecationMessage );
    }

    public static Migration propertyRenamed( String oldKey, String newKey, String deprecationMessage )
    {
        return new PropertyRenamed( oldKey, newKey, deprecationMessage );
    }

    private final List<Migration> migrations = new ArrayList<>();

    public void add( Migration migration )
    {
        migrations.add( migration );
    }

    @Override
    public Map<String,String> apply( Map<String,String> rawConfiguration, Log log )
    {
        boolean printedDeprecationMessage = false;
        for ( Migration migration : migrations )
        {
            if ( migration.appliesTo( rawConfiguration ) )
            {
                if ( !printedDeprecationMessage )
                {
                    printedDeprecationMessage = true;
                    log.warn( "WARNING! Deprecated configuration options used. See manual for details" );
                }
                rawConfiguration = migration.apply( rawConfiguration );
                log.warn( migration.getDeprecationMessage() );
            }
        }
        return rawConfiguration;
    }
}
