/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.helpers.NodeValueIndexCursorAdapter;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

public class ExtendedNodeValueIndexCursorAdapter extends NodeValueIndexCursorAdapter implements EntityIndexSeekClient
{
    @Override
    public void setRead( Read read, Resource resource )
    {
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues,
            boolean indexIncludesTransactionState )
    {
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        return false;
    }

    @Override
    public boolean needsValues()
    {
        return false;
    }
}
