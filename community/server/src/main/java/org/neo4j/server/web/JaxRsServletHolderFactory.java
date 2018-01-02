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
package org.neo4j.server.web;

import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.rest.web.AllowAjaxFilter;

/**
 * Different {@link ServerModule}s can register services at the same mount point.
 * So this class will collect all packages/classes per mount point and create the {@link ServletHolder}
 * when all modules have registered services, see {@link #create(Collection, boolean)}.
 */
public abstract class JaxRsServletHolderFactory
{
    private final List<String> items = new ArrayList<String>();
    private final List<Injectable<?>> injectables = new ArrayList<Injectable<?>>();

    public void add( List<String> items, Collection<Injectable<?>> injectableProviders )
    {
        this.items.addAll( items );
        if ( injectableProviders != null )
        {
            this.injectables.addAll( injectableProviders );
        }
    }

    public void remove( List<String> items )
    {
        this.items.removeAll( items );
    }

    public ServletHolder create( Collection<InjectableProvider<?>> defaultInjectables, boolean wadlEnabled )
    {
        Collection<InjectableProvider<?>> injectableProviders = mergeInjectables( defaultInjectables, injectables );
        ServletContainer container = new NeoServletContainer( injectableProviders );
        ServletHolder servletHolder = new ServletHolder( container );
        servletHolder.setInitParameter( ResourceConfig.FEATURE_DISABLE_WADL, String.valueOf( !wadlEnabled ) );
        configure( servletHolder, toCommaSeparatedList( items ) );
        servletHolder.setInitParameter( ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, AllowAjaxFilter.class.getName() );
        servletHolder.setInitParameter( ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, getRequestFilterConfig() );
        return servletHolder;
    }

    private String getRequestFilterConfig()
    {
        // Ordering of execution of filters goes from left to right
        return XForwardFilter.class.getName();
    }

    protected abstract void configure( ServletHolder servletHolder, String commaSeparatedList );

    private Collection<InjectableProvider<?>> mergeInjectables( Collection<InjectableProvider<?>> defaultInjectables,
            Collection<Injectable<?>> injectables )
    {
        Collection<InjectableProvider<?>> injectableProviders = new ArrayList<InjectableProvider<?>>();
        if ( defaultInjectables != null )
        {
            injectableProviders.addAll( defaultInjectables );
        }
        if ( injectables != null )
        {
            for ( Injectable<?> injectable : injectables )
            {
                injectableProviders.add( new InjectableWrapper( injectable ) );
            }
        }
        return injectableProviders;
    }

    private String toCommaSeparatedList( List<String> packageNames )
    {
        StringBuilder sb = new StringBuilder();

        for ( String str : packageNames )
        {
            sb.append( str );
            sb.append( ", " );
        }

        String result = sb.toString();
        return result.substring( 0, result.length() - 2 );
    }

    public static class Packages extends JaxRsServletHolderFactory
    {
        @Override
        protected void configure( ServletHolder servletHolder, String packages )
        {
            servletHolder.setInitParameter( PackagesResourceConfig.PROPERTY_PACKAGES, packages );
        }
    }

    public static class Classes extends JaxRsServletHolderFactory
    {
        @Override
        protected void configure( ServletHolder servletHolder, String classes )
        {
            servletHolder.setInitParameter( ClassNamesResourceConfig.PROPERTY_CLASSNAMES, classes );
        }
    }
}
