/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

public class VersionedTableIterator<VALUE> extends TableKeyIterator<VALUE>
{
    VersionedTableIterator( Table<VALUE> table, AbstractHopScotchCollection<VALUE> collection )
    {
        super( table, collection );
    }

    @Override
    protected boolean isVisible( int index, long key )
    {
        return super.isVisible( index, key ) &&
                // Was this added after we started the iterator?
                stable.version( index ) <= collection.table.version() &&
                // Has this been removed in the potentially resized table?
                collection.table.key( index ) != nullKey;
    }
}
