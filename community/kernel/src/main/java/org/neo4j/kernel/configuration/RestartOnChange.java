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

package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Specification;
import org.neo4j.helpers.Specifications;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.Specifications.*;
import static org.neo4j.helpers.collection.Iterables.*;

/**
* When a specified change happens, restart the given LifeSupport instance.
 * 
 * Typically, provide a specification for the settings that a service uses, and then set it to restart
 * an internal LifeSupport instance when any of those settings change.
*/
public class RestartOnChange
    implements ConfigurationChangeListener
{
    private final Specification<String> restartSpecification;
    private final Lifecycle life;

    public RestartOnChange(Class<?> settingsClass, Lifecycle life)
    {
        this( or( map( new Function<Field, Specification<String>>()
        {
            @Override
            public Specification<String> map( Field method )
            {
                try
                {
                    GraphDatabaseSetting setting = (GraphDatabaseSetting) method.get( null );
                    return Specifications.in( setting.name() );
                }
                catch( IllegalAccessException e )
                {
                    return Specifications.not( Specifications.<String>TRUE() );
                }
            }
        }, Iterables.iterable( settingsClass.getFields() ) ) ), life);
    }
    
    public RestartOnChange( Specification<String> restartSpecification, Lifecycle life )
    {
        this.restartSpecification = restartSpecification;
        this.life = life;
    }

    @Override
    public void notifyConfigurationChanges( Iterable<ConfigurationChange> change )
    {
        boolean restart = false;
        for( ConfigurationChange configurationChange : change )
        {
            restart |= restartSpecification.satisfiedBy( configurationChange.getName() );
        }

        if (restart)
        {
            try
            {
                life.stop();
                life.start();
            }
            catch( Throwable throwable )
            {
                throwable.printStackTrace();
            }
        }
    }
}
