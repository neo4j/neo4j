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

import org.neo4j.function.Predicate;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * When a specified change happens, restart the given LifeSupport instance.
 * <p>
 * Typically, provide a specification for the settings that a service uses, and then set it to restart
 * an internal LifeSupport instance when any of those settings change.
 */
public class RestartOnChange
        implements ConfigurationChangeListener
{
    private final Predicate<String> restartSpecification;
    private final Lifecycle life;

    public RestartOnChange( final String configurationNamePrefix, Lifecycle life )
    {
        this( new Predicate<String>()
        {
            @Override
            public boolean test( String item )
            {
                return item.startsWith( configurationNamePrefix );
            }
        }, life );
    }

    /**
     * @deprecated use {@link #RestartOnChange(Predicate, Lifecycle)} instead
     */
    @Deprecated
    public RestartOnChange( org.neo4j.helpers.Predicate<String> restartSpecification, Lifecycle life )
    {
        this( org.neo4j.helpers.Predicates.upgrade( restartSpecification ), life );
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
            restart |= restartSpecification.test( configurationChange.getName() );
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
