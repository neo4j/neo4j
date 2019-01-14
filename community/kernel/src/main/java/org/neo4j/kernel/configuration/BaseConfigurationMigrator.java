/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import javax.annotation.Nonnull;

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
    public abstract static class SpecificPropertyMigration implements Migration
    {
        private final String propertyKey;
        private final String deprecationMessage;

        SpecificPropertyMigration( String propertyKey, String deprecationMessage )
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
            String value = rawConfiguration.remove( propertyKey );
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

    private final List<Migration> migrations = new ArrayList<>();

    public void add( Migration migration )
    {
        migrations.add( migration );
    }

    @Override
    @Nonnull
    public Map<String,String> apply( @Nonnull Map<String,String> rawConfiguration, @Nonnull Log log )
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
