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
package org.neo4j.kernel;

import org.hamcrest.Matcher;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.BufferingLogging;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

@SuppressWarnings( "unchecked" )
public class DefaultGraphDatabaseDependenciesTest
{
    @Test
    public void graphDatabaseSettingsIsTheDefaultSettingsClass()
    {
        GraphDatabaseDependencies deps = new DefaultGraphDatabaseDependencies();
        Class<GraphDatabaseSettings>[] settingsClasses = new Class[] { GraphDatabaseSettings.class };
        verifySettingsClasses( deps, settingsClasses );
    }

    @Test
    public void canSpecifyLogger()
    {
        Logging logging = new BufferingLogging();
        GraphDatabaseDependencies deps = new DefaultGraphDatabaseDependencies( logging );
        assertThat( deps.logging(), sameInstance( logging ) );
    }

    @Test
    public void canSpecifySettingsClasses()
    {
        GraphDatabaseDependencies deps = new DefaultGraphDatabaseDependencies( A.class, B.class );
        verifySettingsClasses( deps, A.class, B.class );
    }

    @Test
    public void canSpecifyLoggerAndSettingsClasses()
    {
        Logging logging = new BufferingLogging();
        GraphDatabaseDependencies deps = new DefaultGraphDatabaseDependencies( logging, A.class, B.class );
        assertThat( deps.logging(), sameInstance( logging ) );
        verifySettingsClasses( deps, A.class, B.class );
    }

    private void verifySettingsClasses(
            GraphDatabaseDependencies deps,
            Class<?>... settingsClasses )
    {
        assertThat( deps.settingsClasses(),
                (Matcher) iterableWithSize( settingsClasses.length ) );
        assertThat( deps.settingsClasses(),
                (Matcher) containsInAnyOrder( settingsClasses ) );
    }

    // These are mock settings classes:
    private static class A {}
    private static class B {}
}
