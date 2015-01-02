/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;

/**
 * The basic service implementation for TransactionInterceptorProviders.
 * Offers two ways to instantiate a TransactionInterceptor - one is
 * standalone, the other is with an existing one as the next in the chain
 * of responsibility.
 */
public abstract class TransactionInterceptorProvider extends Service
{
    private final String name;

    public TransactionInterceptorProvider( String name )
    {
        super( name );
        this.name = name;
    }

    /**
     * Returns the name of this provider
     *
     * @return The name of this provider
     */
    public final String name()
    {
        return name;
    }

    /**
     * Creates a TransactionInterceptor with the given datasource and options.
     * It is possible for this method to return null, signifying that the
     * options passed did not allow for instantiation.
     *
     * @param ds The datasource the TransactionInterceptor will communicate with
     * @param options An object that can be the options to instantiate the
     *            interceptor with - e.g "false" to prevent instantiation
     * @return An implementation of TransactionInterceptor or null if the
     *         options say so.
     */
    public abstract TransactionInterceptor create( XaDataSource ds,
            String options, DependencyResolver dependencyResolver );

    /**
     * Creates a TransactionInterceptor with the given datasource and options
     * and the given TransactionInterceptor as the next in the chain.
     * It is possible for this method to return null, signifying that the
     * options passed did not allow for instantiation.
     *
     * @param ds The datasource the TransactionInterceptor will communicate with
     * @param options An object that can be the options to instantiate the
     *            interceptor with - e.g "false" to prevent instantiation
     * @param next The next interceptor in the chain - can be null
     * @return An implementation of TransactionInterceptor or null if the
     *         options say so.
     */
    public abstract TransactionInterceptor create( TransactionInterceptor next,
            XaDataSource ds, String options, DependencyResolver dependencyResolver );
}
