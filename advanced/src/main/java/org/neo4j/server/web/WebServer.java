package org.neo4j.server.web;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

public interface WebServer {
    public void addPackages(Set<String> packageNames);
    public void setPort(int portNo);
    public int getPort();
    public void start();
    public void shutdown();
    public void setMaxThreads(int maxThreads);
    public URI getBaseUri() throws URISyntaxException;
    public URI getWelcomeUri() throws URISyntaxException;
}
