/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import java.util.function.IntPredicate;

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
    public final int createToken( String name )
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

    @Override
    public final void createTokens( String[] names, int[] ids, IntPredicate filter )
    {
        // Making this more efficient (by actually batching) requires a protocol change.
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( filter.test( i ) )
            {
                ids[i] = createToken( names[i] );
            }
        }
    }

    protected abstract Response<Integer> create( Master master, RequestContext context, String name );
}
