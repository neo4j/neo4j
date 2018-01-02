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
package org.neo4j.server.modules;

import org.apache.commons.configuration.Configuration;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.PluginLifecycle;

import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


public class ExtensionInitializerTest
{

    @Test
    public void testPluginInitialization()
    {
        Config config = new Config( stringMap( ServerSettings.transaction_timeout.name(), "600" ) );
        NeoServer neoServer = Mockito.mock( NeoServer.class, Mockito.RETURNS_DEEP_STUBS );
        Mockito.when( neoServer.getConfig() ).thenReturn( config );
        ExtensionInitializer extensionInitializer = new ExtensionInitializer( neoServer );

        Collection<Injectable<?>> injectableProperties =
                extensionInitializer.initializePackages( Arrays.asList( "org.neo4j.server.modules" ) );

        assertThat( injectableProperties, Matchers.hasSize( 1 ) );
        assertThat( injectableProperties, Matchers.contains( new InjectableMatcher<>( ServerSettings
                .transaction_timeout.name() ) ) );
    }

    private class InjectableMatcher<T> extends BaseMatcher<Injectable<?>>
    {
        private T value;

        public InjectableMatcher( T value )
        {
            this.value = value;
        }

        @Override
        public boolean matches( Object o )
        {
            return o instanceof Injectable && value.equals( ((Injectable) o).getValue() );
        }

        @Override
        public void describeMismatch( Object o, Description description )
        {
            description.appendValue( String.format( "Expect Injectable with value: '%s', but actual value was: '%s'",
                    value, o ) );
        }

        @Override
        public void describeTo( Description description )
        {

        }
    }

    public static class PropertyCollectorPlugin implements PluginLifecycle
    {

        @Override
        public Collection<Injectable<?>> start( GraphDatabaseService graphDatabaseService, Configuration config )
        {
            return Iterables.toList( Iterables.map( new StringToInjectableFunction(), config.getKeys() ) );
        }

        @Override
        public void stop()
        {

        }

        private class StringToInjectableFunction implements Function<String,Injectable<?>>
        {

            @Override
            public Injectable<String> apply( final String value )
            {
                return new Injectable<String>()
                {
                    @Override
                    public String getValue()
                    {
                        return value;
                    }

                    @Override
                    public Class<String> getType()
                    {
                        return String.class;
                    }
                };
            }
        }
    }
}