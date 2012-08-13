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
package org.neo4j.server.web;

import java.util.Collection;
import java.util.Set;

import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ConfigurationProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.plugins.PluginInvocatorProvider;
import org.neo4j.server.rest.paging.LeaseManagerProvider;
import org.neo4j.server.rest.repr.InputFormatProvider;
import org.neo4j.server.rest.repr.OutputFormatProvider;
import org.neo4j.server.rest.repr.RepresentationFormatRepository;
import org.neo4j.server.rrd.RrdDbProvider;

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;

@SuppressWarnings( "serial" )
public class NeoServletContainer extends ServletContainer
{
    private final Collection<InjectableProvider<?>> injectables;

    public NeoServletContainer( Collection<InjectableProvider<?>> injectables )
    {
        this.injectables = injectables;
    }

    @Override
    protected void configure( WebConfig wc, ResourceConfig rc, WebApplication wa )
    {
        super.configure( wc, rc, wa );

        Set<Object> singletons = rc.getSingletons();
        for ( InjectableProvider<?> injectable : injectables )
        {
            singletons.add( injectable );
        }
    }

}
