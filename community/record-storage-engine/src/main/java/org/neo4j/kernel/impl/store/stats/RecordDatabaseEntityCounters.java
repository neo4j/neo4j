/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.store.stats;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.pagecache.context.CursorContext;

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public class RecordDatabaseEntityCounters implements StoreEntityCounters
{
    private final IdGeneratorFactory idGeneratorFactory;
    private final CountsAccessor countsAccessor;

    public RecordDatabaseEntityCounters( IdGeneratorFactory idGeneratorFactory, CountsAccessor countsAccessor )
    {
        this.idGeneratorFactory = idGeneratorFactory;
        this.countsAccessor = countsAccessor;
    }

    @Override
    public long nodes()
    {
        return idGeneratorFactory.get( RecordIdType.NODE ).getNumberOfIdsInUse();
    }

    @Override
    public long relationships()
    {
        return idGeneratorFactory.get( RecordIdType.RELATIONSHIP ).getNumberOfIdsInUse();
    }

    @Override
    public long properties()
    {
        return idGeneratorFactory.get( RecordIdType.PROPERTY ).getNumberOfIdsInUse();
    }

    @Override
    public long relationshipTypes()
    {
        return idGeneratorFactory.get( SchemaIdType.RELATIONSHIP_TYPE_TOKEN ).getNumberOfIdsInUse();
    }

    @Override
    public long allNodesCountStore( CursorContext cursorContext )
    {
        return countsAccessor.nodeCount( ANY_LABEL, cursorContext );
    }

    @Override
    public long allRelationshipsCountStore( CursorContext cursorContext )
    {
        return countsAccessor.relationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, cursorContext );
    }
}
