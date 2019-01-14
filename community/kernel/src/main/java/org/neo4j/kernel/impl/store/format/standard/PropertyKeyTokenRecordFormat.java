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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;

public class PropertyKeyTokenRecordFormat extends TokenRecordFormat<PropertyKeyTokenRecord>
{
    public PropertyKeyTokenRecordFormat()
    {
        super( BASE_RECORD_SIZE + 4/*prop count field*/, StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS );
    }

    @Override
    public PropertyKeyTokenRecord newRecord()
    {
        return new PropertyKeyTokenRecord( -1 );
    }

    @Override
    protected void readRecordData( PageCursor cursor, PropertyKeyTokenRecord record, boolean inUse )
    {
        int propertyCount = cursor.getInt();
        int nameId = cursor.getInt();
        record.initialize( inUse, nameId, propertyCount );
    }

    @Override
    protected void writeRecordData( PropertyKeyTokenRecord record, PageCursor cursor )
    {
        cursor.putInt( record.getPropertyCount() );
        cursor.putInt( record.getNameId() );
    }
}
