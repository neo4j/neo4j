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

import ch.qos.logback.access.jetty.RequestLogImpl;
import ch.qos.logback.access.servlet.TeeFilter;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.MovedContextHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.PortBindException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.security.ssl.KeyStoreInformation;
import org.neo4j.server.security.ssl.SslSocketConnectorFactory;

import static java.lang.String.format;

/**
 * This class handles the configuration and runtime management of a Jetty web server. The server is restartable.
 *
 * TODO: it really should be split up into a builder that returns a Closeable, to separate between the conf and runtime part.
 */
public class Jetty9WebServer implements WebServer
{
    private boolean wadlEnabled;
    private Collection<InjectableProvider<?>> defaultInjectables;

    private static class FilterDefinition
    {
        private final Filter filter;
        private final String pathSpec;

        public FilterDefinition( Filter filter, String pathSpec )
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

        public String getPathSpec()
        {
            return pathSpec;
        }
    }

    private static final int DEFAULT_HTTPS_PORT = 7473;
    public static final int DEFAULT_PORT = 80;
    public static final String DEFAULT_ADDRESS = "0.0.0.0";

    private Server jetty;
    private HandlerCollection handlers;
    private int jettyHttpPort = DEFAULT_PORT;
    private int jettyHttpsPort = DEFAULT_HTTPS_PORT;
    private String jettyAddr = DEFAULT_ADDRESS;

    private final HashMap<String, String> staticContent = new HashMap<>();
    private final Map<String, JaxRsServletHolderFactory> jaxRSPackages =
            new HashMap<>();
    private final Map<String, JaxRsServletHolderFactory> jaxRSClasses =
            new HashMap<>();
    private final List<FilterDefinition> filters = new ArrayList<>();

    private int jettyMaxThreads;
    private boolean httpsEnabled = false;
    private KeyStoreInformation httpsCertificateInformation = null;
    private final SslSocketConnectorFactory sslSocketFactory;
    private final HttpConnectorFactory connectorFactory;
    private File requestLoggingConfiguration;
    private final Log log;

    public Jetty9WebServer( LogProvider logProvider, Config config )
    {
        this.log = logProvider.getLog( getClass() );
        sslSocketFactory = new SslSocketConnectorFactory(config);
        connectorFactory = new HttpConnectorFactory(config);
    }

    @Override
    public void start() throws Exception
    {
        if ( jetty == null )
        {
            JettyThreadCalculator jettyThreadCalculator = new JettyThreadCalculator(jettyMaxThreads);
            QueuedThreadPool pool = createQueuedThreadPool( jettyThreadCalculator );

            jetty = new Server( pool );

            jetty.addConnector( connectorFactory.createConnector( jetty, jettyAddr, jettyHttpPort, jettyThreadCalculator ) );

            if ( httpsEnabled )
            {
                if ( httpsCertificateInformation != null )
                {
                    jetty.addConnector(
                            sslSocketFactory.createConnector( jetty, httpsCertificateInformation, jettyAddr,
                                    jettyHttpsPort, jettyThreadCalculator ) );
                }
                else
                {
                    throw new RuntimeException( "HTTPS set to enabled, but no HTTPS configuration provided" );
                }
            }

        }

        handlers = new HandlerList();

        jetty.setHandler( handlers );

        MovedContextHandler redirector = new MovedContextHandler();

        handlers.addHandler( redirector );

        loadAllMounts();

        startJetty();

    }

    private QueuedThreadPool createQueuedThreadPool( JettyThreadCalculator jtc )
    {
        BlockingQueue<Runnable> queue = new BlockingArrayQueue<>( jtc.getMinThreads(), jtc.getMinThreads(), jtc.getMaxCapacity() );
        return new QueuedThreadPool( jtc.getMaxThreads(), jtc.getMinThreads(), 60000, queue );
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
    public void setPort( int portNo )
    {
        jettyHttpPort = portNo;
    }

    @Override
    public void setAddress( String addr )
    {
        jettyAddr = addr;
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

        JaxRsServletHolderFactory factory = jaxRSPackages.get( mountPoint );
        if ( factory == null )
        {
            factory = new JaxRsServletHolderFactory.Packages();
            jaxRSPackages.put( mountPoint, factory );
        }
        factory.add( packageNames, injectables );

        log.debug( "Adding JAXRS packages %s at [%s]", packageNames, mountPoint );
    }

    @Override
    public void addJAXRSClasses( List<String> classNames, String mountPoint, Collection<Injectable<?>> injectables )
    {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri( mountPoint );
        mountPoint = trimTrailingSlashToKeepJettyHappy( mountPoint );

        JaxRsServletHolderFactory factory = jaxRSClasses.get( mountPoint );
        if ( factory == null )
        {
            factory = new JaxRsServletHolderFactory.Classes();
            jaxRSClasses.put( mountPoint, factory );
        }
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
        Iterator<FilterDefinition> iter = filters.iterator();
        while ( iter.hasNext() )
        {
            FilterDefinition current = iter.next();
            if ( current.matches( filter, pathSpec ) )
            {
                iter.remove();
            }
        }
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
    public void setHttpLoggingConfiguration( File logbackConfigFile, boolean enableContentLogging )
    {
        this.requestLoggingConfiguration = logbackConfigFile;
        if(enableContentLogging)
        {
            addFilter( new TeeFilter(), "/*" );
        }
    }

    @Override
    public void setEnableHttps( boolean enable )
    {
        httpsEnabled = enable;
    }

    @Override
    public void setHttpsPort( int portNo )
    {
        jettyHttpsPort = portNo;
    }

    @Override
    public void setHttpsCertificateInformation( KeyStoreInformation config )
    {
        httpsCertificateInformation = config;
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
        catch( BindException e )
        {
            throw new PortBindException( new HostnamePort( jettyAddr, jettyHttpPort ), e );
        }
    }

    private void loadAllMounts()
    {
        SessionManager sm = new HashSessionManager();

        final SortedSet<String> mountpoints = new TreeSet<>( new Comparator<String>()
        {
            @Override
            public int compare( final String o1, final String o2 )
            {
                return o2.compareTo( o1 );
            }
        } );

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
                loadStaticContent( sm, contentKey );
            }
            else if ( isJaxrsPackage )
            {
                loadJAXRSPackage( sm, contentKey );
            }
            else if ( isJaxrsClass )
            {
                loadJAXRSClasses( sm, contentKey );
            }
            else
            {
                throw new RuntimeException( format( "content-key '%s' is not mapped", contentKey ) );
            }
        }

        if ( requestLoggingConfiguration != null )
        {
            loadRequestLogging();
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
        final RequestLogImpl requestLog = new RequestLogImpl();
        requestLog.setFileName( requestLoggingConfiguration.getAbsolutePath() );

        // This makes the request log handler decorate whatever other handlers are already set up
        final RequestLogHandler requestLogHandler = new RequestLogHandler();
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

    private void loadStaticContent( SessionManager sm, String mountPoint )
    {
        String contentLocation = staticContent.get( mountPoint );
        log.info( "Mounting static content at %s", mountPoint );
        try
        {
            SessionHandler sessionHandler = new SessionHandler( sm );
            sessionHandler.setServer( getJetty() );
            final WebAppContext staticContext = new WebAppContext();
            staticContext.setServer( getJetty() );
            staticContext.setContextPath( mountPoint );
            staticContext.setSessionHandler( sessionHandler );
            staticContext.setInitParameter( "org.eclipse.jetty.servlet.Default.dirAllowed", "false" );
            URL resourceLoc = getClass().getClassLoader()
                    .getResource( contentLocation );
            if ( resourceLoc != null )
            {
                URL url = resourceLoc.toURI().toURL();
                final Resource resource = Resource.newResource( url );
                staticContext.setBaseResource( resource );

                addFiltersTo( staticContext );
                staticContext.addFilter( new FilterHolder( new NoCacheHtmlFilter() ), "/*",
                        EnumSet.of( DispatcherType.REQUEST, DispatcherType.FORWARD ) );

                handlers.addHandler( staticContext );
            }
            else
            {
                log.warn(
                        "No static content available for Neo Server at port %d, management console may not be available.",
                        jettyHttpPort );
            }
        }
        catch ( Exception e )
        {
            log.error( "Unknown error loading static content", e );
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    private void loadJAXRSPackage( SessionManager sm, String mountPoint )
    {
        loadJAXRSResource( sm, mountPoint, jaxRSPackages.get( mountPoint ) );
    }

    private void loadJAXRSClasses( SessionManager sm, String mountPoint )
    {
        loadJAXRSResource( sm, mountPoint, jaxRSClasses.get( mountPoint ) );
    }

    private void loadJAXRSResource( SessionManager sm, String mountPoint,
                                    JaxRsServletHolderFactory jaxRsServletHolderFactory )
    {
        SessionHandler sessionHandler = new SessionHandler( sm );
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

}
