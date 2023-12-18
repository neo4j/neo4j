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
package org.neo4j.consistency.store.synthetic;

import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.Mask;

/**
 * Synthetic record type that stands in for a real record to fit in conveniently
 * with consistency checking
 */
public class TokenScanDocument extends AbstractBaseRecord
{
    private EntityTokenRange entityTokenRange;

    public TokenScanDocument( EntityTokenRange entityTokenRange )
    {
        super( entityTokenRange.id() );
        this.entityTokenRange = entityTokenRange;
        setInUse( true );
    }

    @Override
    public void clear()
    {
        super.clear();
        this.entityTokenRange = null;
    }

    public EntityTokenRange getEntityTokenRange()
    {
        return entityTokenRange;
    }

    @Override
    public TokenScanDocument copy()
    {
        throw new UnsupportedOperationException( "Synthetic records cannot be copied." );
    }

    @Override
    public String toString( Mask mask )
    {
        return entityTokenRange.toString();
    }
}
