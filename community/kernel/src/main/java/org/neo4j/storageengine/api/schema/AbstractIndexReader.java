/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;

public abstract class AbstractIndexReader implements IndexReader
{
    protected final SchemaIndexDescriptor descriptor;

    protected AbstractIndexReader( SchemaIndexDescriptor descriptor )
    {
        this.descriptor = descriptor;
    }

    @Override
    public void query(
            IndexProgressor.NodeValueClient client,
            IndexOrder indexOrder,
            IndexQuery... query ) throws IndexNotApplicableKernelException
    {
        if ( indexOrder != IndexOrder.NONE )
        {
            throw new UnsupportedOperationException(
                    String.format( "This reader only have support for index order %s. Provided index order was %s.",
                            IndexOrder.NONE, indexOrder ) );
        }
        client.initialize( descriptor, new NodeValueIndexProgressor( query( query ), client ), query );
    }

}
