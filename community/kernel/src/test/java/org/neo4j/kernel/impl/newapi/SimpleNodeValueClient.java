/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

public class SimpleNodeValueClient implements IndexProgressor.NodeValueClient
{
    public long reference;
    public Value[] values;
    private IndexProgressor progressor;

    public boolean next()
    {
        return progressor.next();
    }

    @Override
    public void initialize( IndexProgressor progressor, int[] propertyIds )
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
    public void done()
    {
        reference = 0;
        values = null;
        progressor = null;
    }
}
