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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestConfigConcurrency
{

    /**
     * Hammers config with read/write load on both properties and the listener
     * interface it provides.
     */
    class ConfigHammer implements Runnable, ConfigurationChangeListener
    {
        private final Config config;
        private final Random rand;
        protected Throwable failure;

        public ConfigHammer(Config config)
        {
            this.config = config;
            this.rand = new Random(  );
        }

        @Override
        public void run()
        {
            try
            {
                int times = 500;
                while ( times --> 0 )
                {
                    config.addConfigurationChangeListener( this );

                    // Edit config a bit
                    Map<String,String> params = config.getParams();
                    params.put( "asd" + rand.nextInt( 10 ),"dsa" + rand.nextInt( 100000 ) );

                    config.applyChanges( params );

                    // Unregister listener
                    config.removeConfigurationChangeListener( this );
                }
            } catch(Throwable e)
            {
                this.failure = e;
            }
        }

        @Override
        public void notifyConfigurationChanges( Iterable<ConfigurationChange> change )
        {
        }
    }

    @Test(timeout = 10000l)
    public void shouldHandleConcurrentLoad() throws Throwable
    {
        // Given
        Config config = new Config();

        List<Thread> threads = new ArrayList<>(  );
        List<ConfigHammer> hammers = new ArrayList<>(  );

        // When
        int numThreads = 10;
        while( numThreads --> 0)
        {
            ConfigHammer configHammer = new ConfigHammer( config );
            Thread thread = new Thread( configHammer );
            thread.start();

            threads.add( thread );
            hammers.add( configHammer );
        }

        // Then
        for ( Thread thread : threads )
            thread.join();

        // And no hammer has broken
        for ( ConfigHammer hammer : hammers )
            if(hammer.failure != null)
                throw hammer.failure;
    }
}
