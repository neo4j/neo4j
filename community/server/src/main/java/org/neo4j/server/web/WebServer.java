/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.ssl.SslPolicy;

public interface WebServer
{
    void setAddress( ListenSocketAddress address );

    void setHttpsAddress( Optional<ListenSocketAddress> address );

    void setSslPolicy( SslPolicy sslPolicy );

    void setRequestLog( RequestLog requestLog );

    void setMaxThreads( int maxThreads );

    void start() throws Exception;

    void stop();

    void addJAXRSPackages( List<String> packageNames, String serverMountPoint, Collection<Injectable<?>> injectables );
    void removeJAXRSPackages( List<String> packageNames, String serverMountPoint );

    void addJAXRSClasses( List<String> classNames, String serverMountPoint, Collection<Injectable<?>> injectables );
    void removeJAXRSClasses( List<String> classNames, String serverMountPoint );

    void addFilter( Filter filter, String pathSpec );

    void removeFilter( Filter filter, String pathSpec );

    void addStaticContent( String contentLocation, String serverMountPoint );
    void removeStaticContent( String contentLocation, String serverMountPoint );

    void invokeDirectly( String targetUri, HttpServletRequest request, HttpServletResponse response )
        throws IOException, ServletException;

    void setWadlEnabled( boolean wadlEnabled );

    void setDefaultInjectables( Collection<InjectableProvider<?>> defaultInjectables );

    void setJettyCreatedCallback( Consumer<Server> callback );

    /**
     * @return local http connector bind port
     */
    InetSocketAddress getLocalHttpAddress();

    /**
     * @return local https connector bind port
     */
    InetSocketAddress getLocalHttpsAddress();
}
