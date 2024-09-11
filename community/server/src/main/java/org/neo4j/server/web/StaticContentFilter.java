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

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

public class StaticContentFilter implements Filter {

    private final String contentSecurityPolicyHeader;

    public StaticContentFilter(String contentSecurityPolicyHeader) {
        this.contentSecurityPolicyHeader = contentSecurityPolicyHeader;
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // redirects pass through this filter twice so we check this request hasn't already been filtered
        if (response.getHeader("Cache-Control") == null) {
            response.addHeader("Cache-Control", "no-store");
            response.addHeader("Pragma", "no-cache");
            response.addHeader("Content-Security-Policy", contentSecurityPolicyHeader);
            response.addHeader("X-Frame-Options", "DENY");
            response.addHeader("X-Content-Type-Options", "nosniff");
            response.addHeader("X-XSS-Protection", "1; mode=block");
        }
        System.out.println(servletResponse);
        filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() {}
}
