/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import org.neo4j.server.database.TransactionRegistry;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class TransactionFilter implements Filter
{

    public static final String SESSION_HEADER   = "X-Session";
    public static final String TX_ACTION_HEADER = "X-Tx-Action";

    private final TransactionRegistry registry;

    private enum Action
    {
        COMMIT,
        ROLLBACK,
        NONE;
    }

    public TransactionFilter(TransactionRegistry registry)
    {

        this.registry = registry;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException
    {
        if(servletRequest instanceof HttpServletRequest)
        {
            HttpServletRequest req = (HttpServletRequest) servletRequest;
            HttpServletResponse res = (HttpServletResponse) servletResponse;
            Long sessionId = getSessionId(req);
            if(sessionId != null)
            {
                registry.associateTransactionWithThread(sessionId);

                switch(getTxAction(req))
                {
                    case COMMIT:
                        filterChain.doFilter(servletRequest, servletResponse);
                        registry.commitCurrentTransaction(sessionId);
                        break;
                    case ROLLBACK:
                        registry.rollbackCurrentTransaction(sessionId);
                        res.setStatus(200);
                        break;
                    default:
                        filterChain.doFilter(servletRequest, servletResponse);
                        registry.disassociateTransactionWithThread(sessionId);
                        break;
                }
            } else
            {
                // No session
                filterChain.doFilter(servletRequest, servletResponse);
            }
        }


    }

    private Action getTxAction(HttpServletRequest req)
    {
        String value = req.getHeader(TX_ACTION_HEADER);
        if(value == null || value.length() == 0)
        {
            return Action.NONE;
        }

        try {
            switch (value.getBytes("UTF-8")[0])
            {
                case 'C': return Action.COMMIT;
                case 'R': return Action.ROLLBACK;
                default:  return Action.NONE;
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private Long getSessionId(HttpServletRequest req) {
        String value = req.getHeader(SESSION_HEADER);
        if(value != null)
        {
            return Long.valueOf(value);
        }
        return null;
    }

    @Override
    public void destroy()
    {
    }
}
