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
package org.dummy.web.service;

import java.util.Collection;
import java.util.Collections;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.PluginLifecycle;

public class DummyPluginInitializer implements PluginLifecycle
{
    public DummyPluginInitializer()
    {
    }

    @Override
    public Collection<Injectable<?>> start( GraphDatabaseService graphDatabaseService, Configuration config )
    {
        return Collections.<Injectable<?>>singleton( new Injectable<Long>()
        {
            @Override
            public Long getValue()
            {
                return 42L;

            }

            @Override
            public Class<Long> getType()
            {
                return Long.class;
            }
        } );
    }

    @Override
    public void stop()
    {
    }
}
