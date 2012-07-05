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

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.component.LifeCycle;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionManager;
import org.mortbay.jetty.handler.MovedContextHandler;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.HashSessionManager;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.SessionHandler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.resource.Resource;
import org.mortbay.thread.QueuedThreadPool;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.guard.GuardingRequestFilter;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rest.security.SecurityFilter;
import org.neo4j.server.rest.security.SecurityRule;
import org.neo4j.server.rest.security.UriPathWildcardMatcher;
import org.neo4j.server.rest.web.AllowAjaxFilter;
import org.neo4j.server.security.KeyStoreInformation;
import org.neo4j.server.security.SslSocketConnectorFactory;

import ch.qos.logback.access.jetty.RequestLogImpl;

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class Jetty6WebServer implements WebServer
{
	private static class FilterDefinition
	{
		private final Filter filter;
		private final String pathSpec;

		public FilterDefinition(Filter filter, String pathSpec)
		{
			this.filter = filter;
			this.pathSpec = pathSpec;
		}
		
		public boolean matches(Filter filter, String pathSpec)
		{
			return filter == this.filter && pathSpec.equals(this.pathSpec);
		}

		public Filter getFilter() {
			return filter;
		}

		public String getPathSpec() {
			return pathSpec;
		}
	}
	
    private static final int DEFAULT_HTTPS_PORT = 7473;
    public static final Logger log = Logger.getLogger( Jetty6WebServer.class );
    public static final int DEFAULT_PORT = 80;
    public static final String DEFAULT_ADDRESS = "0.0.0.0";

    private Server jetty;
    private int jettyHttpPort = DEFAULT_PORT;
    private int jettyHttpsPort = DEFAULT_HTTPS_PORT;
    private String jettyAddr = DEFAULT_ADDRESS;

    private final HashMap<String, String> staticContent = new HashMap<String, String>();
    private final HashMap<String, ServletHolder> jaxRSPackages = new HashMap<String, ServletHolder>();
    private final List<FilterDefinition> filters = new ArrayList<FilterDefinition>();
    
    private NeoServer server;
    private int jettyMaxThreads = tenThreadsPerProcessor();
    private boolean httpsEnabled = false;
    private KeyStoreInformation httpsCertificateInformation = null;
    private final SslSocketConnectorFactory sslSocketFactory = new SslSocketConnectorFactory();
	private File requestLoggingConfiguration;

    @Override
    public void init()
    {
    }
    
    @Override
    public void start()
    {
        if ( jetty == null )
        {
            jetty = new Server();
            Connector connector = new SelectChannelConnector();

            connector.setPort( jettyHttpPort );
            connector.setHost( jettyAddr );

            jetty.addConnector( connector );

            if ( httpsEnabled )
            {
                if ( httpsCertificateInformation != null )
                {
                    jetty.addConnector(
                        sslSocketFactory.createConnector( httpsCertificateInformation, jettyAddr, jettyHttpsPort ) );
                }
                else
                {
                    throw new RuntimeException( "HTTPS set to enabled, but no HTTPS configuration provided." );
                }
            }

            jetty.setThreadPool( new QueuedThreadPool( jettyMaxThreads ) );
        }
        
        MovedContextHandler redirector = new MovedContextHandler();

        jetty.addHandler( redirector );

        loadAllMounts();

        startJetty();
    }

    @Override
    public void stop()
    {
        try
        {
            jetty.stop();
            jetty.join();
            jetty = null;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
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
    public void addJAXRSPackages( List<String> packageNames, String mountPoint )
    {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri( mountPoint );

        mountPoint = trimTrailingSlashToKeepJettyHappy( mountPoint );

        ServletContainer container = new NeoServletContainer( server, server.getInjectables( packageNames ) );
        ServletHolder servletHolder = new ServletHolder( container );
        if ( !Boolean.valueOf( String.valueOf( server.getConfiguration().getProperty( Configurator.WADL_ENABLED ) ) ) )
        {
            servletHolder.setInitParameter( ResourceConfig.FEATURE_DISABLE_WADL, String.valueOf( true ) );
        }
        servletHolder.setInitParameter( "com.sun.jersey.config.property.packages",
            toCommaSeparatedList( packageNames ) );
        servletHolder.setInitParameter( ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS,
            AllowAjaxFilter.class.getName() );
        log.debug( "Adding JAXRS packages %s at [%s]", packageNames, mountPoint );

        jaxRSPackages.put( mountPoint, servletHolder );
    }
    
    @Override
	public void removeJAXRSPackages( List<String> packageNames, String serverMountPoint )
    {
    	jaxRSPackages.remove(serverMountPoint);
    }

    @Override
    public void addFilter(Filter filter, String pathSpec)
    {
    	filters.add(new FilterDefinition(filter, pathSpec));
    }
    
    @Override
    public void removeFilter(Filter filter, String pathSpec)
    {
    	Iterator<FilterDefinition> iter = filters.iterator();
    	while(iter.hasNext())
    	{
    		FilterDefinition current = iter.next();
    		if(current.matches(filter, pathSpec))
    		{
    			iter.remove();
    		}
    	}
    }

    @Override
    public void setNeoServer( NeoServer server )
    {
        this.server = server;
    }

	@Override
    public void addStaticContent( String contentLocation, String serverMountPoint )
    {
        staticContent.put( serverMountPoint, contentLocation );
    }
    
    @Override
	public void removeStaticContent( String contentLocation, String serverMountPoint )
    {
    	staticContent.remove(serverMountPoint);
    }

    @Override
    public void invokeDirectly( String targetPath, HttpServletRequest request, HttpServletResponse response )
        throws IOException, ServletException
    {
        jetty.handle( targetPath, request, response, Handler.REQUEST );
    }

    @Override
    public void setHttpLoggingConfiguration( File logbackConfigFile )
    {
    	this.requestLoggingConfiguration = logbackConfigFile;
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

    protected void startJetty()
    {
        try
        {
            jetty.start();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private int tenThreadsPerProcessor()
    {
        return 10 * Runtime.getRuntime()
            .availableProcessors();
    }

    private void loadAllMounts()
    {
        SessionManager sm = new HashSessionManager();

        final SortedSet<String> mountpoints = new TreeSet<String>( new Comparator<String>()
        {
            @Override
            public int compare( final String o1, final String o2 )
            {
                return o2.compareTo( o1 );
            }
        } );
        
        if(requestLoggingConfiguration != null)
        {
        	loadRequestLogging();
        }

        mountpoints.addAll( staticContent.keySet() );
        mountpoints.addAll( jaxRSPackages.keySet() );

        for ( String contentKey : mountpoints )
        {
            final boolean isStatic = staticContent.containsKey( contentKey );
            final boolean isJaxrs = jaxRSPackages.containsKey( contentKey );

            if ( isStatic && isJaxrs )
            {
                throw new RuntimeException(
                    format( "content-key '%s' is mapped twice (static and jaxrs)", contentKey ) );
            }
            else if ( isStatic )
            {
                loadStaticContent( sm, contentKey );
            }
            else if ( isJaxrs )
            {
                loadJAXRSPackage( sm, contentKey );
            }
            else
            {
                throw new RuntimeException( format( "content-key '%s' is not mapped", contentKey ) );
            }
        }
    }
    
    private void loadRequestLogging() {
        final RequestLogImpl requestLog = new RequestLogImpl();
        requestLog.setFileName( requestLoggingConfiguration.getAbsolutePath() );

        final RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog( requestLog );

        jetty.addHandler( requestLogHandler );
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
            log.debug( "Unable to translate [%s] to a relative URI in ensureRelativeUri(String mountPoint)",
                mountPoint );
            return mountPoint;
        }
    }

    private void loadStaticContent( SessionManager sm, String mountPoint )
    {
        String contentLocation = staticContent.get( mountPoint );
        log.info( "Mounting static content at [%s] from [%s]", mountPoint, contentLocation );
        try
        {
            final WebAppContext staticContext = new WebAppContext( null, new SessionHandler( sm ), null, null );
            staticContext.setServer( getJetty() );
            staticContext.setContextPath( mountPoint );
            URL resourceLoc = getClass().getClassLoader()
                .getResource( contentLocation );
            if ( resourceLoc != null )
            {
                log.debug( "Found [%s]", resourceLoc );
                URL url = resourceLoc.toURI()
                    .toURL();
                final Resource resource = Resource.newResource( url );
                staticContext.setBaseResource( resource );
                log.debug( "Mounting static content from [%s] at [%s]", url, mountPoint );
                
                addFiltersTo(staticContext);
                
                jetty.addHandler( staticContext );
            }
            else
            {
                log.error(
                    "No static content available for Neo Server at port [%d], management console may not be available.",
                    jettyHttpPort );
            }
        }
        catch ( Exception e )
        {
            log.error( e );
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

	private void loadJAXRSPackage( SessionManager sm, String mountPoint )
    {
        ServletHolder servletHolder = jaxRSPackages.get( mountPoint );
        log.debug( "Mounting servlet at [%s]", mountPoint );
        Context jerseyContext = new Context( jetty, mountPoint );
        SessionHandler sh = new SessionHandler( sm );
        jerseyContext.addServlet( servletHolder, "/*" );
        jerseyContext.setSessionHandler( sh );
        addFiltersTo(jerseyContext);
    }

    private void addFiltersTo(Context context) {
    	for(FilterDefinition filterDef : filters)
    	{
            context.addFilter( new FilterHolder( 
            		filterDef.getFilter() ), 
            		filterDef.getPathSpec(), Handler.ALL );
    	}
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

    @Override
    public void addSecurityRules( final SecurityRule... rules )
    {
        jetty.addLifeCycleListener( new JettyLifeCycleListenerAdapter()
        {
            @Override
            public void lifeCycleStarted( LifeCycle arg0 )
            {
                for ( Handler handler : jetty.getHandlers() )
                {
                    if ( handler instanceof Context )
                    {
                        final Context context = (Context) handler;
                        for ( SecurityRule rule : rules )
                        {
                            if ( new UriPathWildcardMatcher( rule.forUriPath() ).matches( context.getContextPath() ) )
                            {
                                final Filter jettyFilter = new SecurityFilter( rule );
                                context.addFilter( new FilterHolder( jettyFilter ), "/*", Handler.ALL );
                                log.info( "Security rule [%s] installed on server",
                                    rule.getClass().getCanonicalName() );
                                System.out.println( String.format( "Security rule [%s] installed on server",
                                    rule.getClass().getCanonicalName() ) );
                            }
                        }
                    }
                }
            }
        } );
    }

    @Override
    public void addExecutionLimitFilter( final int timeout )
    {
        final Guard guard = server.getDatabase().getGraph().getGuard();
        if ( guard == null )
        {
            //TODO enable guard and restart EmbeddedGraphdb
            throw new RuntimeException( "unable to use guard, enable guard-insertion in neo4j.properties" );
        }

        jetty.addLifeCycleListener( new JettyLifeCycleListenerAdapter()
        {
            @Override
            public void lifeCycleStarted( LifeCycle arg0 )
            {
                for ( Handler handler : jetty.getHandlers() )
                {
                    if ( handler instanceof Context )
                    {
                        final Context context = (Context) handler;
                        final Filter jettyFilter = new GuardingRequestFilter( guard, timeout );
                        final FilterHolder holder = new FilterHolder( jettyFilter );
                        context.addFilter( holder, "/*", Handler.ALL );
                    }
                }
            }
        } );
    }
}
