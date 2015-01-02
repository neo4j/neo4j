/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Arrays;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.Predicates.or;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * When a specified change happens, restart the given LifeSupport instance.
 * <p/>
 * Typically, provide a specification for the settings that a service uses, and then set it to restart
 * an internal LifeSupport instance when any of those settings change.
 */
public class RestartOnChange
        implements ConfigurationChangeListener
{
    private final Predicate<String> restartSpecification;
    private final Lifecycle life;

    public RestartOnChange( Class<?> settingsClass, Lifecycle life )
    {
        this( or( map( new Function<Field, Predicate<String>>()
        {
            @Override
            public Predicate<String> apply( Field method )
            {
                try
                {
                    Setting setting = (Setting) method.get( null );
                    return Predicates.in( setting.name() );
                }
                catch ( IllegalAccessException e )
                {
                    return Predicates.not( Predicates.<String>TRUE() );
                }
            }
        }, Arrays.asList( settingsClass.getFields() ) ) ), life );
    }

    public RestartOnChange( final String configurationNamePrefix, Lifecycle life )
    {
        this( new Predicate<String>()
        {
            @Override
            public boolean accept( String item )
            {
                return item.startsWith( configurationNamePrefix );
            }
        }, life );
    }

    public RestartOnChange( Predicate<String> restartSpecification, Lifecycle life )
    {
        this.restartSpecification = restartSpecification;
        this.life = life;
    }

    @Override
    public void notifyConfigurationChanges( Iterable<ConfigurationChange> change )
    {
        boolean restart = false;
        for ( ConfigurationChange configurationChange : change )
        {
            restart |= restartSpecification.accept( configurationChange.getName() );
        }

        if ( restart )
        {
            try
            {
                life.stop();
                life.start();
            }
            catch ( Throwable throwable )
            {
                throwable.printStackTrace();
            }
        }
    }
}
