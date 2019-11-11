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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Map;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexDropper;
import org.neo4j.values.storable.Value;

class FulltextIndexDropper implements IndexDropper
{
    private final IndexDescriptor index;
    private final DatabaseIndex<FulltextIndexReader> fulltextIndex;
    private final boolean readOnly;

    FulltextIndexDropper( IndexDescriptor index, DatabaseIndex<FulltextIndexReader> fulltextIndex, boolean readOnly )
    {
        this.index = index;
        this.fulltextIndex = fulltextIndex;
        this.readOnly = readOnly;
    }

    @Override
    public void drop()
    {
        if ( readOnly )
        {
            throw new IllegalStateException( "Cannot drop read-only index." );
        }
        fulltextIndex.drop();
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return index.getIndexConfig().asMap();
    }
}
