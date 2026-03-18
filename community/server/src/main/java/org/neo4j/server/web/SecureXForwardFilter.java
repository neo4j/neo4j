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

import static org.neo4j.server.configuration.ServerSettings.http_x_forward_allow_hosts;
import static org.neo4j.server.configuration.ServerSettings.http_x_forward_allow_proxies;
import static org.neo4j.server.configuration.ServerSettings.http_x_forward_enabled;
import static org.neo4j.server.configuration.ServerSettings.http_x_forward_private_ips_enabled;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_HOST_HEADER_KEY;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_PROTO_HEADER_KEY;

import java.net.URI;
import java.util.Set;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * Secure implementation of X-Forwarded-Host and X-Forwarded-Proto header processing.
 * <p>
 * This filter provides security controls to prevent host header injection attacks:
 * - Can be completely disabled (recommended for direct deployments)
 * - Validates source IP addresses against trusted proxy list
 * - Validates hostnames against allowlist
 * - Blocks private IP addresses unless explicitly allowed
 * - Logs suspicious activity for security monitoring
 */
@PreMatching
public class SecureXForwardFilter implements ContainerRequestFilter {
    private final boolean enabled;
    private final Set<String> trustedProxies;
    private final Set<String> allowedHosts;
    private final boolean allowPrivateIps;
    private final InternalLog log;

    @Context
    private Provider<HttpServletRequest> httpRequestProvider;

    public SecureXForwardFilter(Config config, InternalLogProvider logProvider) {
        this.enabled = config.get(http_x_forward_enabled);
        this.trustedProxies = Set.copyOf(config.get(http_x_forward_allow_proxies));
        this.allowedHosts = Set.copyOf(config.get(http_x_forward_allow_hosts));
        this.allowPrivateIps = config.get(http_x_forward_private_ips_enabled);
        this.log = logProvider.getLog(getClass());

        if (enabled) {
            log.info("X-Forward header processing enabled with security controls");
            if (trustedProxies.isEmpty()) {
                log.warn("SECURITY WARNING: X-Forward headers accepted from any source - configure allow_proxies");
            }
            if (allowedHosts.isEmpty()) {
                log.warn("SECURITY WARNING: X-Forward headers accept any hostname - configure allow_hosts");
            }
        } else {
            log.debug("X-Forward header processing disabled (secure default)");
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        // SECURITY: Exit early if disabled (fail-safe default)
        if (!enabled) {
            return;
        }

        String clientIP = extractClientIP(requestContext);

        // SECURITY: Only trust configured proxy sources
        if (!trustedProxies.isEmpty() && !trustedProxies.contains(clientIP)) {
            log.warn("Untrusted source " + clientIP + " attempted to send X-Forward headers");
            return;
        }

        String xForwardedHost = requestContext.getHeaderString(X_FORWARD_HOST_HEADER_KEY);
        String xForwardedProto = requestContext.getHeaderString(X_FORWARD_PROTO_HEADER_KEY);

        // Validate X-Forwarded-Host
        if (xForwardedHost != null && !isValidHost(xForwardedHost, clientIP)) {
            return; // Validation failed, reject silently
        }

        // Process validated headers
        if (xForwardedHost != null || xForwardedProto != null) {
            UriInfo uriInfo = requestContext.getUriInfo();
            URI externalBaseUri = XForwardUtil.externalUri(uriInfo.getBaseUri(), xForwardedHost, xForwardedProto);
            URI externalRequestUri = XForwardUtil.externalUri(uriInfo.getRequestUri(), xForwardedHost, xForwardedProto);

            requestContext.setRequestUri(externalBaseUri, externalRequestUri);

            log.debug("Applied X-Forward headers from trusted source " + clientIP + ": host=" + xForwardedHost
                    + ", proto=" + xForwardedProto);
        }
    }

    private String extractClientIP(ContainerRequestContext requestContext) {
        // Use Provider for request-scoped access so the filter works when shared across mounts
        if (httpRequestProvider != null) {
            try {
                HttpServletRequest httpRequest = httpRequestProvider.get();
                if (httpRequest != null) {
                    return httpRequest.getRemoteAddr();
                }
            } catch (IllegalStateException e) {
                // Not inside a request scope (e.g. shared filter instance)
            }
        }

        // Fallback for unit tests or when request is unavailable
        return "127.0.0.1";
    }

    private boolean isValidHost(String hostHeader, String clientIP) {
        if (hostHeader == null || hostHeader.trim().isEmpty()) {
            return false;
        }

        // Extract first host from comma-separated list (same as XForwardUtil)
        String firstHost = hostHeader.split(",")[0].trim();

        // Extract hostname (remove port if present)
        String hostname = firstHost.split(":")[0].trim();

        // Check allowlist if configured
        if (!allowedHosts.isEmpty() && !allowedHosts.contains(hostname)) {
            log.warn("Rejected X-Forwarded-Host '" + XForwardSecurityUtil.sanitizeForLogging(hostname) + "' from "
                    + clientIP + ": not in allowed hosts");
            return false;
        }

        // Block private IPs unless explicitly allowed
        if (!allowPrivateIps && XForwardSecurityUtil.isPrivateIP(hostname)) {
            log.warn("Rejected X-Forwarded-Host '" + XForwardSecurityUtil.sanitizeForLogging(firstHost) + "' from "
                    + clientIP + ": private IP address blocked");
            return false;
        }

        // Additional validation - check for obviously malicious patterns
        if (XForwardSecurityUtil.containsMaliciousPatterns(firstHost)) {
            log.warn("Rejected X-Forwarded-Host '" + XForwardSecurityUtil.sanitizeForLogging(firstHost) + "' from "
                    + clientIP + ": contains malicious patterns");
            return false;
        }

        return true;
    }
}
