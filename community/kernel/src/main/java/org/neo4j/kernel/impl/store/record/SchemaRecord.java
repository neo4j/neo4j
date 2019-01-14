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
package org.neo4j.kernel.impl.store.record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

public class SchemaRecord extends AbstractBaseRecord implements Iterable<DynamicRecord>
{
    private Collection<DynamicRecord> records;

    public SchemaRecord( Collection<DynamicRecord> records )
    {
        super( -1 );
        initialize( records );
    }

    public SchemaRecord initialize( Collection<DynamicRecord> records )
    {
        initialize( true );
        Iterator<DynamicRecord> iterator = records.iterator();
        long id = iterator.hasNext() ? iterator.next().getId() : NULL_REFERENCE.intValue();
        setId( id );
        this.records = records;
        return this;
    }

    public void setDynamicRecords( Collection<DynamicRecord> records )
    {
        this.records.clear();
        this.records.addAll( records );
    }

    @Override
    public void clear()
    {
        super.clear();
        this.records = null;
    }

    @Override
    public Iterator<DynamicRecord> iterator()
    {
        return records.iterator();
    }

    public int size()
    {
        return records.size();
    }

    @Override
    public SchemaRecord clone()
    {
        List<DynamicRecord> list = new ArrayList<>( records.size() );
        for ( DynamicRecord record : records )
        {
            list.add( record.clone() );
        }
        return new SchemaRecord( list );
    }
}
