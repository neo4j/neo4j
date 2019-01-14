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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;

import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.RecordCursors;

/**
 * Cursor for a specific property on a node or relationship.
 */
public class StoreSinglePropertyCursor extends StorePropertyCursor
{
    private int propertyKeyId = StatementConstants.NO_SUCH_PROPERTY_KEY;

    public StoreSinglePropertyCursor( RecordCursors cursors, Consumer<StoreSinglePropertyCursor> instanceCache )
    {
        super( cursors, (Consumer) instanceCache );
    }

    public StoreSinglePropertyCursor init( long firstPropertyId, int propertyKeyId, Lock lock, AssertOpen assertOpen )
    {
        super.init( firstPropertyId, lock, assertOpen );
        this.propertyKeyId = propertyKeyId;
        return this;
    }

    @Override
    public boolean next()
    {
        try
        {
            if ( propertyKeyId != StatementConstants.NO_SUCH_PROPERTY_KEY )
            {
                while ( super.next() )
                {
                    if ( get().propertyKeyId() == this.propertyKeyId )
                    {
                        return true;
                    }
                }
            }

            return false;
        }
        finally
        {
            this.propertyKeyId = StatementConstants.NO_SUCH_PROPERTY_KEY;
        }
    }
}
