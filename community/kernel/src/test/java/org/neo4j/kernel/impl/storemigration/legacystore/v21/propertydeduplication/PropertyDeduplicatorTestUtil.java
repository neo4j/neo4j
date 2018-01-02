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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class PropertyDeduplicatorTestUtil
{
    public static Map<String, Integer> indexPropertyKeys( PropertyKeyTokenStore propertyKeyTokenStore )
    {
        Map<String, Integer> propertyKeyIndex = new HashMap<>();
        long highestPossibleIdInUse = propertyKeyTokenStore.getHighestPossibleIdInUse();
        for ( int i = 0; i <= highestPossibleIdInUse; i++ )
        {
            Token token = propertyKeyTokenStore.getToken( i );
            propertyKeyIndex.put( token.name(), token.id() );
        }
        return propertyKeyIndex;
    }

    public static Token findTokenFor( TokenStore tokenStore, String key )
    {
        long highestPossibleIdInUse = tokenStore.getHighestPossibleIdInUse();
        for ( int i = 0; i <= highestPossibleIdInUse; i++ )
        {
            Token token = tokenStore.getToken( i );
            if ( token.name().equals( key ) )
            {
                return token;
            }
        }
        return null;
    }

    public static void replacePropertyKey(
            PropertyStore propertyStore,
            PrimitiveRecord record,
            Token original,
            Token replacement )
    {
        long nextProp = record.getNextProp();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propertyRecord = propertyStore.getRecord( nextProp );
            for ( PropertyBlock propertyBlock : propertyRecord )
            {
                if ( propertyBlock.getKeyIndexId() == original.id() )
                {
                    propertyBlock.setKeyIndexId( replacement.id() );
                }
            }
            propertyStore.updateRecord( propertyRecord );
            nextProp = propertyRecord.getNextProp();
        }
    }
}
