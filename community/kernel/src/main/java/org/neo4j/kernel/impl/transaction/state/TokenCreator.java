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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.api.Token;

import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;

public class TokenCreator<R extends TokenRecord, T extends Token>
{
    private final TokenStore<R, T> store;

    public TokenCreator( TokenStore<R, T> store )
    {
        this.store = store;
    }

    public void createToken( String name, int id, RecordAccess<R,Void> recordAccess )
    {
        R record = recordAccess.create( id, null ).forChangingData();
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> nameRecords = store.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) Iterables.first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
    }
}
