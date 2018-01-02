/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.TokenCreator;

public abstract class AbstractTokenCreator implements TokenCreator
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;

    protected AbstractTokenCreator( Master master, RequestContextFactory requestContextFactory )
    {
        this.master = master;
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public final int getOrCreate( String name )
    {
        try ( Response<Integer> response = create( master, requestContextFactory.newRequestContext(), name ) )
        {
            return response.response();
        }
        catch ( ComException e )
        {
            throw new TransientTransactionFailureException(
                    "Cannot create identifier for token '" + name + "' on the master " + master + ". " +
                    "The master is either down, or we have network connectivity problems", e );
        }
    }

    protected abstract Response<Integer> create( Master master, RequestContext context, String name );
}
