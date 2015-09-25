/*
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
package org.neo4j.server.rrd.sampler;

import org.neo4j.kernel.impl.transaction.state.NeoStoreSupplier;
import org.neo4j.server.rrd.UnableToSampleException;

public class NodeIdsInUseSampleable extends DatabasePrimitivesSampleableBase
{

    public NodeIdsInUseSampleable( NeoStoreSupplier neoStoreSupplier )
    {
        super( neoStoreSupplier );
    }

    @Override public String getName()
    {
        return "node_count";
    }

    @Override public double getValue()
    {
        try
        {
            return getNeoStore().getNodeStore().getNumberOfIdsInUse();
        }
        catch ( Exception e )
        {
            throw new UnableToSampleException( "Unexpected exception caught while sampling", e );
        }
    }
}
