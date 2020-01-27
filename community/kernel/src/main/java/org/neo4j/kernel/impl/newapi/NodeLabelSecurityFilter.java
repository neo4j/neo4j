/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

class NodeLabelSecurityFilter implements IndexProgressor.EntityValueClient, IndexProgressor
{
    private final int[] properties;
    private final EntityValueClient target;
    private final NodeCursor node;
    private final Read read;
    private final AccessMode accessMode;
    private IndexProgressor progressor;

    NodeLabelSecurityFilter( int[] properties, EntityValueClient target, NodeCursor node, Read read, AccessMode accessMode )
    {
        this.properties = properties;
        this.target = target;
        this.node = node;
        this.read = read;
        this.accessMode = accessMode;
    }

    @Override
    public boolean next()
    {
        return progressor.next();
    }

    @Override
    public void close()
    {
        IOUtils.close( RuntimeException::new, node, progressor );
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexQueryConstraints constraints,
            boolean indexIncludesTransactionState )
    {
        this.progressor = progressor;
        target.initialize( descriptor, this, query, constraints, indexIncludesTransactionState );
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        read.singleNode( reference, node );
        if ( !node.next() )
        {
            // This node is not visible to this security context
            return false;
        }

        boolean allowed = true;
        long[] labels = node.labelsIgnoringTxStateSetRemove().all();
        for ( int prop : properties )
        {
            allowed &= accessMode.allowsReadNodeProperty( () -> Labels.from( labels ), prop );
        }

        return allowed && target.acceptEntity( reference, score, values );
    }

    @Override
    public boolean needsValues()
    {
        return target.needsValues();
    }
}
