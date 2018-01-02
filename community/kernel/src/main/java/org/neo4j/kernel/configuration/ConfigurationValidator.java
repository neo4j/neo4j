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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Pair;

/**
 * Given a set of annotated config classes,
 * validates configuration maps using the validators
 * in the setting class fields.
 */
public class ConfigurationValidator
{

    private AnnotatedFieldHarvester fieldHarvester = new AnnotatedFieldHarvester();
    private Map<String, Setting<?>> settings;

    public ConfigurationValidator( Iterable<Class<?>> settingsClasses )
    {
        this.settings = getSettingsFrom( settingsClasses );
    }

    public void validate( Map<String, String> rawConfig )
    {
        for ( Setting<?> setting : settings.values() )
        {
            setting.apply( Functions.map( rawConfig ) );
        }
    }

    @SuppressWarnings("rawtypes")
    private Map<String, Setting<?>> getSettingsFrom( Iterable<Class<?>> settingsClasses )
    {
        Map<String, Setting<?>> settings = new HashMap<>();
        for ( Class<?> clazz : settingsClasses )
        {
            for ( Pair<Field, Setting> field : fieldHarvester.findStatic( clazz, Setting.class ) )
            {
                settings.put( field.other().name(), field.other() );
            }
        }
        return settings;
    }

}
