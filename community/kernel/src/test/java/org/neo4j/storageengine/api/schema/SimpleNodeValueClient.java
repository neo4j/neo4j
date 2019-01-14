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

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.Value;

public class SimpleNodeValueClient implements IndexProgressor.NodeValueClient
{
    public long reference;
    public Value[] values;
    private IndexProgressor progressor;

    public boolean next()
    {
        if ( progressor.next() )
        {
            return true;
        }
        closeProgressor();
        return false;
    }

    @Override
    public void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query )
    {
        this.progressor = progressor;
    }

    @Override
    public boolean acceptNode( long reference, Value... values )
    {
        this.reference = reference;
        this.values = values;
        return true;
    }

    @Override
    public boolean needsValues()
    {
        return true;
    }

    private void closeProgressor()
    {
        if ( progressor != null )
        {
            progressor.close();
            progressor = null;
        }
    }
}
