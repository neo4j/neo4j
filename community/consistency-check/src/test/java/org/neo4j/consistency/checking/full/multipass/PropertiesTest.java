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
package org.neo4j.consistency.checking.full.multipass;

import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class PropertiesTest extends MultiPassStoreAbstractTest
{
    @Override
    protected MultiPassStore multiPassStore()
    {
        return MultiPassStore.PROPERTIES;
    }

    @Override
    protected RecordReference<PropertyRecord> record( RecordAccess filter, long id )
    {
        return filter.property( id, NULL );
    }

    @Override
    protected void otherRecords( RecordAccess filter, long id )
    {
        filter.node( id, NULL );
        filter.relationship( id, NULL );
        filter.string( id, NULL );
        filter.array( id, NULL );
    }
}
