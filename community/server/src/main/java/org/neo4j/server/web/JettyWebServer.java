/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.web;

import static java.lang.String.format;

import java.io.IOException;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import org.eclipse.jetty.io.ByteBufferPool;
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
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.CommonConnectorConfig;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.server.bind.ComponentsBinder;
import org.neo4j.server.security.ssl.SslSocketConnectorFactory;
import org.neo4j.ssl.SslPolicy;

/**
 * This class handles the configuration and runtime management of a Jetty web server. The server is restartable.
 */
public class JettyWebServer implements WebServer, WebContainerThreadInfo {
    private static final int JETTY_THREAD_POOL_IDLE_TIMEOUT = 60000;

    public static final SocketAddress DEFAULT_ADDRESS = new SocketAddress("0.0.0.0", 80);

    private boolean wadlEnabled;
    private ComponentsBinder binder;
    private RequestLog requestLog;

    private Server jetty;
    private HandlerCollection handlers;
    private SocketAddress httpAddress = DEFAULT_ADDRESS;
    private SocketAddress httpsAddress;

    private ServerConnector httpConnector;
    private ServerConnector httpsConnector;

    private final Map<String, String> staticContent = new HashMap<>();
    private final Map<String, JaxRsServletHolderFactory> jaxRsServletHolderFactories = new HashMap<>();
    private final List<FilterDefinition> filters = new ArrayList<>();

    private int jettyMaxThreads = 1;
    private SslPolicy sslPolicy;
    private final boolean ocspStaplingEnabled;
    private final SslSocketConnectorFactory sslSocketFactory;
    private final HttpConnectorFactory connectorFactory;
    private final InternalLog log;

    public JettyWebServer(
            InternalLogProvider logProvider,
            Config config,
            NetworkConnectionTracker connectionTracker,
            ByteBufferPool byteBufferPool) {
        this.log = logProvider.getLog(getClass());
        this.ocspStaplingEnabled = config.get(CommonConnectorConfig.ocsp_stapling_enabled);
        sslSocketFactory = new SslSocketConnectorFactory(connectionTracker, config, byteBufferPool);
        connectorFactory = new HttpConnectorFactory(connectionTracker, config, byteBufferPool);
    }

    @Override
    public void start() throws Exception {
        if (jetty == null) {
            verifyAddressConfiguration();

            JettyThreadCalculator jettyThreadCalculator = new JettyThreadCalculator(jettyMaxThreads);
            jetty = new Server(createQueuedThreadPool(jettyThreadCalculator));

            if (httpAddress != null) {
                httpConnector = connectorFactory.createConnector(jetty, httpAddress, jettyThreadCalculator);
                jetty.addConnector(httpConnector);
            }

            if (httpsAddress != null) {
                if (sslPolicy == null) {
                    throw new RuntimeException("HTTPS set to enabled, but no SSL policy provided");
                }

                if (ocspStaplingEnabled) {
                    // currently the only way to enable OCSP server stapling for JDK is through this property
                    System.setProperty("jdk.tls.server.enableStatusRequestExtension", "true");
                }
                httpsConnector =
                        sslSocketFactory.createConnector(jetty, sslPolicy, httpsAddress, jettyThreadCalculator);
                jetty.addConnector(httpsConnector);
            }
        }

        handlers = new HandlerList();
        jetty.setHandler(handlers);
        handlers.addHandler(new MovedContextHandler());

        loadAllMounts();

        if (requestLog != null) {
            loadRequestLogging();
        }

        startJetty();
    }

    private static QueuedThreadPool createQueuedThreadPool(JettyThreadCalculator jtc) {
        BlockingQueue<Runnable> queue =
                new BlockingArrayQueue<>(jtc.getMinThreads(), jtc.getMinThreads(), jtc.getMaxCapacity());
        QueuedThreadPool threadPool =
                new QueuedThreadPool(jtc.getMaxThreads(), jtc.getMinThreads(), JETTY_THREAD_POOL_IDLE_TIMEOUT, queue);
        threadPool.setThreadPoolBudget(null); // mute warnings about Jetty thread pool size
        return threadPool;
    }

    @Override
    public void stop() {
        if (jetty != null) {
            try {
                jetty.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                jetty.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for Jetty to stop");
            }
            jetty = null;
        }
    }

    @Override
    public void setHttpAddress(SocketAddress address) {
        httpAddress = address;
    }

    @Override
    public void setHttpsAddress(SocketAddress address) {
        httpsAddress = address;
    }

    @Override
    public void setSslPolicy(SslPolicy policy) {
        sslPolicy = policy;
    }

    @Override
    public void setMaxThreads(int maxThreads) {
        jettyMaxThreads = maxThreads;
    }

    @Override
    public void addJAXRSPackages(List<String> packageNames, String mountPoint, Collection<Injectable<?>> injectables) {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri(mountPoint);
        mountPoint = trimTrailingSlashToKeepJettyHappy(mountPoint);

        JaxRsServletHolderFactory factory =
                jaxRsServletHolderFactories.computeIfAbsent(mountPoint, k -> new JaxRsServletHolderFactory());
        factory.addPackages(packageNames, injectables);

        log.debug("Adding JAXRS packages %s at [%s]", packageNames, mountPoint);
    }

    @Override
    public void addJAXRSClasses(List<Class<?>> classNames, String mountPoint, Collection<Injectable<?>> injectables) {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri(mountPoint);
        mountPoint = trimTrailingSlashToKeepJettyHappy(mountPoint);

        JaxRsServletHolderFactory factory =
                jaxRsServletHolderFactories.computeIfAbsent(mountPoint, k -> new JaxRsServletHolderFactory());
        factory.addClasses(classNames, injectables);

        log.debug("Adding JAXRS classes %s at [%s]", classNames, mountPoint);
    }

    @Override
    public void setWadlEnabled(boolean wadlEnabled) {
        this.wadlEnabled = wadlEnabled;
    }

    @Override
    public void setComponentsBinder(ComponentsBinder binder) {
        this.binder = binder;
    }

    @Override
    public void removeJAXRSPackages(List<String> packageNames, String serverMountPoint) {
        JaxRsServletHolderFactory factory = jaxRsServletHolderFactories.get(serverMountPoint);
        if (factory != null) {
            factory.removePackages(packageNames);
        }
    }

    @Override
    public void removeJAXRSClasses(List<Class<?>> classNames, String serverMountPoint) {
        JaxRsServletHolderFactory factory = jaxRsServletHolderFactories.get(serverMountPoint);
        if (factory != null) {
            factory.removeClasses(classNames);
        }
    }

    @Override
    public void addFilter(Filter filter, String pathSpec) {
        filters.add(new FilterDefinition(filter, pathSpec));
    }

    @Override
    public void removeFilter(Filter filter, String pathSpec) {
        filters.removeIf(current -> current.matches(filter, pathSpec));
    }

    @Override
    public void addStaticContent(String contentLocation, String serverMountPoint) {
        staticContent.put(serverMountPoint, contentLocation);
    }

    @Override
    public void removeStaticContent(String contentLocation, String serverMountPoint) {
        staticContent.remove(serverMountPoint);
    }

    @Override
    public void setRequestLog(RequestLog requestLog) {
        this.requestLog = requestLog;
    }

    public Server getJetty() {
        return jetty;
    }

    private void startJetty() throws Exception {
        try {
            jetty.start();
        } catch (IOException e) {
            throw new PortBindException(httpAddress, httpsAddress, e);
        }
    }

    @Override
    public InetSocketAddress getLocalHttpAddress() {
        return getAddress("HTTP", httpConnector);
    }

    @Override
    public InetSocketAddress getLocalHttpsAddress() {
        return getAddress("HTTPS", httpsConnector);
    }

    private void loadAllMounts() {
        final SortedSet<String> mountpoints = new TreeSet<>(Comparator.reverseOrder());

        mountpoints.addAll(staticContent.keySet());
        mountpoints.addAll(jaxRsServletHolderFactories.keySet());

        for (String contentKey : mountpoints) {
            final boolean isStatic = staticContent.containsKey(contentKey);
            final boolean isJaxRs = jaxRsServletHolderFactories.containsKey(contentKey);

            if (isStatic && isJaxRs) {
                throw new RuntimeException(format("content-key '%s' is mapped more than once", contentKey));
            } else if (isStatic) {
                loadStaticContent(contentKey);
            } else if (isJaxRs) {
                loadJaxRsResource(contentKey);
            } else {
                throw new RuntimeException(format("content-key '%s' is not mapped", contentKey));
            }
        }
    }

    private void loadRequestLogging() {
        // This makes the request log handler decorate whatever other handlers are already set up
        final RequestLogHandler requestLogHandler = new HttpChannelOptionalRequestLogHandler();
        requestLogHandler.setRequestLog(requestLog);
        requestLogHandler.setServer(jetty);
        requestLogHandler.setHandler(jetty.getHandler());
        jetty.setHandler(requestLogHandler);
    }

    private static String trimTrailingSlashToKeepJettyHappy(String mountPoint) {
        if (mountPoint.equals("/")) {
            return mountPoint;
        }

        if (mountPoint.endsWith("/")) {
            mountPoint = mountPoint.substring(0, mountPoint.length() - 1);
        }
        return mountPoint;
    }

    private String ensureRelativeUri(String mountPoint) {
        try {
            URI result = new URI(mountPoint);
            if (result.isAbsolute()) {
                return result.getPath();
            } else {
                return result.toString();
            }
        } catch (URISyntaxException e) {
            log.debug("Unable to translate [%s] to a relative URI in ensureRelativeUri(String mountPoint)", mountPoint);
            return mountPoint;
        }
    }

    private void loadStaticContent(String mountPoint) {
        String contentLocation = staticContent.get(mountPoint);
        try {
            SessionHandler sessionHandler = new SessionHandler();
            sessionHandler.setServer(getJetty());
            final WebAppContext staticContext = new WebAppContext();
            staticContext.setServer(getJetty());
            staticContext.setContextPath(mountPoint);
            staticContext.setSessionHandler(sessionHandler);
            staticContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
            URL resourceLoc = getClass().getClassLoader().getResource(contentLocation);
            if (resourceLoc != null) {
                URL url = resourceLoc.toURI().toURL();
                final Resource resource = Resource.newResource(url);
                staticContext.setBaseResource(resource);

                addFiltersTo(staticContext);
                staticContext.addFilter(
                        new FilterHolder(new StaticContentFilter()),
                        "/*",
                        EnumSet.of(DispatcherType.REQUEST, DispatcherType.FORWARD));

                handlers.addHandler(staticContext);
            }
        } catch (Exception e) {
            log.error("Unknown error loading static content", e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void loadJaxRsResource(String mountPoint) {
        log.debug("Mounting servlet at [%s]", mountPoint);

        SessionHandler sessionHandler = new SessionHandler();
        sessionHandler.setServer(getJetty());
        JaxRsServletHolderFactory jaxRsServletHolderFactory = jaxRsServletHolderFactories.get(mountPoint);
        ServletContextHandler jerseyContext = new ServletContextHandler();
        jerseyContext.setServer(getJetty());
        jerseyContext.setErrorHandler(new NeoJettyErrorHandler());
        jerseyContext.setContextPath(mountPoint);
        jerseyContext.setSessionHandler(sessionHandler);
        jerseyContext.addServlet(jaxRsServletHolderFactory.create(binder, wadlEnabled), "/*");
        addFiltersTo(jerseyContext);
        handlers.addHandler(jerseyContext);
    }

    private void addFiltersTo(ServletContextHandler context) {
        for (FilterDefinition filterDef : filters) {

            // If we mount a filter at root serving all subdomains then the filter will be triggered on
            // all endpoints that are not matched by other servlet contexts, which is not what we want.
            if (context.getContextPath().equals("/")) {
                context.addFilter(new FilterHolder(filterDef.getFilter()), "/", EnumSet.allOf(DispatcherType.class));
            } else {
                context.addFilter(
                        new FilterHolder(filterDef.getFilter()),
                        filterDef.getPathSpec(),
                        EnumSet.allOf(DispatcherType.class));
            }
        }
    }

    private static InetSocketAddress getAddress(String name, ServerConnector connector) {
        if (connector == null) {
            throw new IllegalStateException(name + " connector is not configured");
        }
        return new InetSocketAddress(connector.getHost(), connector.getLocalPort());
    }

    private void verifyAddressConfiguration() {
        if (httpAddress == null && httpsAddress == null) {
            throw new IllegalStateException("Either HTTP or HTTPS address must be configured to run the server");
        }
    }

    @Override
    public int allThreads() {
        if (getJetty() != null) {
            return getJetty().getThreadPool().getThreads();
        }
        return -1;
    }

    @Override
    public int idleThreads() {
        if (getJetty() != null) {
            return getJetty().getThreadPool().getIdleThreads();
        }
        return -1;
    }

    private static class FilterDefinition {
        private final Filter filter;
        private final String pathSpec;

        FilterDefinition(Filter filter, String pathSpec) {
            this.filter = filter;
            this.pathSpec = pathSpec;
        }

        public boolean matches(Filter filter, String pathSpec) {
            return filter == this.filter && pathSpec.equals(this.pathSpec);
        }

        public Filter getFilter() {
            return filter;
        }

        String getPathSpec() {
            return pathSpec;
        }
    }
}
