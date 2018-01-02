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
package org.neo4j.server.plugins;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.Representation;

class PluginMethod extends PluginPoint
{
    private final ServerPlugin plugin;
    private final Method method;
    private final DataExtractor[] extractors;
    private final ResultConverter result;

    PluginMethod( String name, Class<?> discovery, ServerPlugin plugin, ResultConverter result, Method method,
            DataExtractor[] extractors, Description description )
    {
        super( discovery, name, description == null ? "" : description.value() );
        this.plugin = plugin;
        this.result = result;
        this.method = method;
        this.extractors = extractors;
    }

    @Override
    public Representation invoke( GraphDatabaseAPI graphDb, Object source, ParameterList params )
            throws BadPluginInvocationException, PluginInvocationFailureException, BadInputException
    {
        Object[] arguments = new Object[extractors.length];
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < arguments.length; i++ )
            {
                arguments[i] = extractors[i].extract( graphDb, source, params );
            }
        }
        try
        {
            Object returned = method.invoke( plugin, arguments );

            if ( returned == null )
            {
                return Representation.emptyRepresentation();
            }
            return result.convert( returned );
        }
        catch ( InvocationTargetException exc )
        {
            Throwable targetExc = exc.getTargetException();
            for ( Class<?> excType : method.getExceptionTypes() )
            {
                if ( excType.isInstance( targetExc ) )
                {
                    throw new BadPluginInvocationException( targetExc );
                }
            }
            throw new PluginInvocationFailureException( targetExc );
        }
        catch ( IllegalArgumentException | IllegalAccessException e )
        {
            throw new PluginInvocationFailureException( e );
        }
    }

    @Override
    protected void describeParameters( ParameterDescriptionConsumer consumer )
    {
        for ( DataExtractor extractor : extractors )
        {
            extractor.describe( consumer );
        }
    }

}
