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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

class RelationshipSecurityFilter implements IndexProgressor.EntityValueClient, IndexProgressor
{
    private final EntityValueClient target;
    private final Read read;
    private final RelationshipScanCursor relCursor;
    private final AccessMode accessMode;
    private final int[] properties;
    private IndexProgressor progressor;

    RelationshipSecurityFilter( int[] properties, EntityValueClient target, RelationshipScanCursor relCursor, Read read, AccessMode accessMode )
    {
        this.target = target;
        this.read = read;
        this.relCursor = relCursor;
        this.accessMode = accessMode;
        this.properties = properties;
    }

    @Override
    public boolean next()
    {
        return progressor.next();
    }

    @Override
    public void close()
    {
        IOUtils.close( RuntimeException::new, relCursor, progressor );
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues,
            boolean indexIncludesTransactionState )
    {
        this.progressor = progressor;
        target.initialize( descriptor, this, query, indexOrder, needsValues, indexIncludesTransactionState );
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        read.singleRelationship( reference, relCursor );
        if ( !relCursor.next() )
        {
            return false;
        }

        int relType = relCursor.type();
        for ( int prop : properties )
        {
            // TODO this filters out too many...
            if ( !accessMode.allowsReadRelationshipProperty( () -> relType, prop ) )
            {
                return false;
            }
        }
        return target.acceptEntity( reference, score, values );
    }

    @Override
    public boolean needsValues()
    {
        return target.needsValues();
    }
}
