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

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.MovedContextHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.PortBindException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.security.ssl.SslSocketConnectorFactory;
import org.neo4j.ssl.SslPolicy;

import static java.lang.String.format;

/**
 * This class handles the configuration and runtime management of a Jetty web server. The server is restartable.
 *
 * TODO: it really should be split up into a builder that returns a Closeable, to separate between the conf and runtime part.
 */
public class Jetty9WebServer implements WebServer
{
    private static final int JETTY_THREAD_POOL_IDLE_TIMEOUT = 60000;

    public static final ListenSocketAddress DEFAULT_ADDRESS = new ListenSocketAddress( "0.0.0.0", 80 );

    private boolean wadlEnabled;
    private Collection<InjectableProvider<?>> defaultInjectables;
    private Consumer<Server> jettyCreatedCallback;
    private RequestLog requestLog;

    private Server jetty;
    private HandlerCollection handlers;
    private ListenSocketAddress jettyAddress = DEFAULT_ADDRESS;
    private Optional<ListenSocketAddress> jettyHttpsAddress = Optional.empty();

    private ServerConnector serverConnector;
    private ServerConnector secureServerConnector;

    private final HashMap<String, String> staticContent = new HashMap<>();
    private final Map<String, JaxRsServletHolderFactory> jaxRSPackages = new HashMap<>();
    private final Map<String, JaxRsServletHolderFactory> jaxRSClasses = new HashMap<>();
    private final List<FilterDefinition> filters = new ArrayList<>();

    private int jettyMaxThreads = 1;
    private SslPolicy sslPolicy;
    private final SslSocketConnectorFactory sslSocketFactory;
    private final HttpConnectorFactory connectorFactory;
    private final Log log;

    public Jetty9WebServer( LogProvider logProvider, Config config )
    {
        this.log = logProvider.getLog( getClass() );
        sslSocketFactory = new SslSocketConnectorFactory( config );
        connectorFactory = new HttpConnectorFactory( config );
    }

    @Override
    public void start() throws Exception
    {
        if ( jetty == null )
        {
            JettyThreadCalculator jettyThreadCalculator = new JettyThreadCalculator( jettyMaxThreads );

            jetty = new Server( createQueuedThreadPool( jettyThreadCalculator ) );
            serverConnector = connectorFactory.createConnector( jetty, jettyAddress, jettyThreadCalculator );
            jetty.addConnector( serverConnector );

            jettyHttpsAddress.ifPresent( address ->
            {
                if ( sslPolicy == null )
                {
                    throw new RuntimeException( "HTTPS set to enabled, but no SSL policy provided" );
                }
                secureServerConnector = sslSocketFactory.createConnector( jetty, sslPolicy, address, jettyThreadCalculator );
                jetty.addConnector( secureServerConnector );
            } );

            if ( jettyCreatedCallback != null )
            {
                jettyCreatedCallback.accept( jetty );
            }
        }

        handlers = new HandlerList();
        jetty.setHandler( handlers );
        handlers.addHandler( new MovedContextHandler() );

        loadAllMounts();

        if ( requestLog != null )
        {
            loadRequestLogging();
        }

        startJetty();
    }

    private static QueuedThreadPool createQueuedThreadPool( JettyThreadCalculator jtc )
    {
        BlockingQueue<Runnable> queue = new BlockingArrayQueue<>( jtc.getMinThreads(), jtc.getMinThreads(), jtc.getMaxCapacity() );
        QueuedThreadPool threadPool = new QueuedThreadPool( jtc.getMaxThreads(), jtc.getMinThreads(), JETTY_THREAD_POOL_IDLE_TIMEOUT, queue );
        threadPool.setThreadPoolBudget( null ); // mute warnings about Jetty thread pool size
        return threadPool;
    }

    @Override
    public void stop()
    {
        if ( jetty != null )
        {
            try
            {
                jetty.stop();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            try
            {
                jetty.join();
            }
            catch ( InterruptedException e )
            {
                log.warn( "Interrupted while waiting for Jetty to stop" );
            }
            jetty = null;
        }
    }

    @Override
    public void setAddress( ListenSocketAddress address )
    {
        jettyAddress = address;
    }

    @Override
    public void setMaxThreads( int maxThreads )
    {
        jettyMaxThreads = maxThreads;
    }

    @Override
    public void addJAXRSPackages( List<String> packageNames, String mountPoint, Collection<Injectable<?>> injectables )
    {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri( mountPoint );
        mountPoint = trimTrailingSlashToKeepJettyHappy( mountPoint );

        JaxRsServletHolderFactory factory = jaxRSPackages.computeIfAbsent( mountPoint, k -> new JaxRsServletHolderFactory.Packages() );
        factory.add( packageNames, injectables );

        log.debug( "Adding JAXRS packages %s at [%s]", packageNames, mountPoint );
    }

    @Override
    public void addJAXRSClasses( List<String> classNames, String mountPoint, Collection<Injectable<?>> injectables )
    {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri( mountPoint );
        mountPoint = trimTrailingSlashToKeepJettyHappy( mountPoint );

        JaxRsServletHolderFactory factory = jaxRSClasses.computeIfAbsent( mountPoint, k -> new JaxRsServletHolderFactory.Classes() );
        factory.add( classNames, injectables );

        log.debug( "Adding JAXRS classes %s at [%s]", classNames, mountPoint );
    }

    @Override
    public void setWadlEnabled( boolean wadlEnabled )
    {
        this.wadlEnabled = wadlEnabled;
    }

    @Override
    public void setDefaultInjectables( Collection<InjectableProvider<?>> defaultInjectables )
    {
        this.defaultInjectables = defaultInjectables;
    }

    @Override
    public void setJettyCreatedCallback( Consumer<Server> callback )
    {
        this.jettyCreatedCallback = callback;
    }

    @Override
    public void removeJAXRSPackages( List<String> packageNames, String serverMountPoint )
    {
        JaxRsServletHolderFactory factory = jaxRSPackages.get( serverMountPoint );
        if ( factory != null )
        {
            factory.remove( packageNames );
        }
    }

    @Override
    public void removeJAXRSClasses( List<String> classNames, String serverMountPoint )
    {
        JaxRsServletHolderFactory factory = jaxRSClasses.get( serverMountPoint );
        if ( factory != null )
        {
            factory.remove( classNames );
        }
    }

    @Override
    public void addFilter( Filter filter, String pathSpec )
    {
        filters.add( new FilterDefinition( filter, pathSpec ) );
    }

    @Override
    public void removeFilter( Filter filter, String pathSpec )
    {
        filters.removeIf( current -> current.matches( filter, pathSpec ) );
    }

    @Override
    public void addStaticContent( String contentLocation, String serverMountPoint )
    {
        staticContent.put( serverMountPoint, contentLocation );
    }

    @Override
    public void removeStaticContent( String contentLocation, String serverMountPoint )
    {
        staticContent.remove( serverMountPoint );
    }

    @Override
    public void invokeDirectly( String targetPath, HttpServletRequest request, HttpServletResponse response )
            throws IOException, ServletException
    {
        jetty.handle( targetPath, (Request) request, request, response );
    }

    @Override
    public void setRequestLog( RequestLog requestLog )
    {
        this.requestLog = requestLog;
    }

    @Override
    public void setHttpsAddress( Optional<ListenSocketAddress> address )
    {
        jettyHttpsAddress = address;
    }

    @Override
    public void setSslPolicy( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }

    public Server getJetty()
    {
        return jetty;
    }

    protected void startJetty() throws Exception
    {
        try
        {
            jetty.start();
        }
        catch ( BindException e )
        {
            if ( jettyHttpsAddress.isPresent() )
            {
                throw new PortBindException( jettyAddress, jettyHttpsAddress.get(), e );
            }
            else
            {
                throw new PortBindException( jettyAddress, e );
            }
        }
    }

    @Override
    public InetSocketAddress getLocalHttpAddress()
    {
        return toSocketAddress( serverConnector );
    }

    @Override
    public InetSocketAddress getLocalHttpsAddress()
    {
        return Optional.ofNullable( secureServerConnector)
                        .map( Jetty9WebServer::toSocketAddress )
                        .orElseThrow( () -> new IllegalStateException( "Secure connector is not configured" ) );
    }

    private void loadAllMounts()
    {
        final SortedSet<String> mountpoints = new TreeSet<>( Comparator.reverseOrder() );

        mountpoints.addAll( staticContent.keySet() );
        mountpoints.addAll( jaxRSPackages.keySet() );
        mountpoints.addAll( jaxRSClasses.keySet() );

        for ( String contentKey : mountpoints )
        {
            final boolean isStatic = staticContent.containsKey( contentKey );
            final boolean isJaxrsPackage = jaxRSPackages.containsKey( contentKey );
            final boolean isJaxrsClass = jaxRSClasses.containsKey( contentKey );

            if ( countSet( isStatic, isJaxrsPackage, isJaxrsClass ) > 1 )
            {
                throw new RuntimeException(
                        format( "content-key '%s' is mapped more than once", contentKey ) );
            }
            else if ( isStatic )
            {
                loadStaticContent( contentKey );
            }
            else if ( isJaxrsPackage )
            {
                loadJAXRSPackage( contentKey );
            }
            else if ( isJaxrsClass )
            {
                loadJAXRSClasses( contentKey );
            }
            else
            {
                throw new RuntimeException( format( "content-key '%s' is not mapped", contentKey ) );
            }
        }
    }

    private int countSet( boolean... booleans )
    {
        int count = 0;
        for ( boolean bool : booleans )
        {
            if ( bool )
            {
                count++;
            }
        }
        return count;
    }

    private void loadRequestLogging()
    {
        // This makes the request log handler decorate whatever other handlers are already set up
        final RequestLogHandler requestLogHandler = new HttpChannelOptionalRequestLogHandler();
        requestLogHandler.setRequestLog( requestLog );
        requestLogHandler.setServer( jetty );
        requestLogHandler.setHandler( jetty.getHandler() );
        jetty.setHandler( requestLogHandler );
    }

    private String trimTrailingSlashToKeepJettyHappy( String mountPoint )
    {
        if ( mountPoint.equals( "/" ) )
        {
            return mountPoint;
        }

        if ( mountPoint.endsWith( "/" ) )
        {
            mountPoint = mountPoint.substring( 0, mountPoint.length() - 1 );
        }
        return mountPoint;
    }

    private String ensureRelativeUri( String mountPoint )
    {
        try
        {
            URI result = new URI( mountPoint );
            if ( result.isAbsolute() )
            {
                return result.getPath();
            }
            else
            {
                return result.toString();
            }
        }
        catch ( URISyntaxException e )
        {
            log.debug( "Unable to translate [%s] to a relative URI in ensureRelativeUri(String mountPoint)", mountPoint );
            return mountPoint;
        }
    }

    private void loadStaticContent( String mountPoint )
    {
        String contentLocation = staticContent.get( mountPoint );
        try
        {
            SessionHandler sessionHandler = new SessionHandler();
            sessionHandler.setServer( getJetty() );
            final WebAppContext staticContext = new WebAppContext();
            staticContext.setServer( getJetty() );
            staticContext.setContextPath( mountPoint );
            staticContext.setSessionHandler( sessionHandler );
            staticContext.setInitParameter( "org.eclipse.jetty.servlet.Default.dirAllowed", "false" );
            URL resourceLoc = getClass().getClassLoader().getResource( contentLocation );
            if ( resourceLoc != null )
            {
                URL url = resourceLoc.toURI().toURL();
                final Resource resource = Resource.newResource( url );
                staticContext.setBaseResource( resource );

                addFiltersTo( staticContext );
                staticContext.addFilter( new FilterHolder( new StaticContentFilter() ), "/*",
                        EnumSet.of( DispatcherType.REQUEST, DispatcherType.FORWARD ) );

                handlers.addHandler( staticContext );
            }
            else
            {
                log.warn( "No static content available for Neo Server at %s, management console may not be available.",
                        jettyAddress );
            }
        }
        catch ( Exception e )
        {
            log.error( "Unknown error loading static content", e );
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    private void loadJAXRSPackage( String mountPoint )
    {
        loadJAXRSResource( mountPoint, jaxRSPackages.get( mountPoint ) );
    }

    private void loadJAXRSClasses( String mountPoint )
    {
        loadJAXRSResource( mountPoint, jaxRSClasses.get( mountPoint ) );
    }

    private void loadJAXRSResource( String mountPoint,
                                    JaxRsServletHolderFactory jaxRsServletHolderFactory )
    {
        SessionHandler sessionHandler = new SessionHandler();
        sessionHandler.setServer( getJetty() );
        log.debug( "Mounting servlet at [%s]", mountPoint );
        ServletContextHandler jerseyContext = new ServletContextHandler();
        jerseyContext.setServer( getJetty() );
        jerseyContext.setErrorHandler( new NeoJettyErrorHandler() );
        jerseyContext.setContextPath( mountPoint );
        jerseyContext.setSessionHandler( sessionHandler );
        jerseyContext.addServlet( jaxRsServletHolderFactory.create( defaultInjectables, wadlEnabled ), "/*" );
        addFiltersTo( jerseyContext );
        handlers.addHandler( jerseyContext );
    }

    private void addFiltersTo( ServletContextHandler context )
    {
        for ( FilterDefinition filterDef : filters )
        {
            context.addFilter( new FilterHolder( filterDef.getFilter() ),
                    filterDef.getPathSpec(), EnumSet.allOf( DispatcherType.class )
            );
        }
    }

    private static InetSocketAddress toSocketAddress( ServerConnector connector )
    {
        return new InetSocketAddress( connector.getHost(), connector.getLocalPort() );
    }

    private static class FilterDefinition
    {
        private final Filter filter;
        private final String pathSpec;

        FilterDefinition( Filter filter, String pathSpec )
        {
            this.filter = filter;
            this.pathSpec = pathSpec;
        }

        public boolean matches( Filter filter, String pathSpec )
        {
            return filter == this.filter && pathSpec.equals( this.pathSpec );
        }

        public Filter getFilter()
        {
            return filter;
        }
        String getPathSpec()
        {
            return pathSpec;
        }
    }

}
