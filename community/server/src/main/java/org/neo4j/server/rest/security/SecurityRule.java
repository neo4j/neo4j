package org.neo4j.server.rest.security;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.server.configuration.Configurator;

public interface SecurityRule
{
    String DEFAULT_DATABASE_PATH = Configurator.DEFAULT_DATA_API_PATH;

    /**
     * @param request The HTTP request currently under consideration.
     * @return <code>true</code> if the rule passes, <code>false</code> if the
     *         rule fails and the request is to be rejected.
     */
    boolean isAuthorized( HttpServletRequest request );

    /**
     * @return the root of the URI path from which rules will be valid, e.g.
     *         <code>/db/data</code> will apply this rule to everything below
     *         the path <code>/db/data</code>
     */
    String forUriPath();

    /**
     * @return the opaque string representing the realm for which the rule
     *         applies. Will be used to formulate a correct <code>401</code>
     *         response code if the rule denies a request.
     */
    String forRealm();
}
