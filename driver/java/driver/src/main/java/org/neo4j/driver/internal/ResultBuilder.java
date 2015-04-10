/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.StreamCollector;

public class ResultBuilder implements StreamCollector
{
    private List<Record> body = new ArrayList<>();
    private Map<String,Integer> fieldLookup = Collections.EMPTY_MAP;

    @Override
    public void fieldNames( String[] names )
    {
        if ( names.length == 0 )
        {
            fieldLookup = Collections.EMPTY_MAP;
        }
        Map<String,Integer> fieldLookup = new HashMap<>();
        for ( int i = 0; i < names.length; i++ )
        {
            fieldLookup.put( names[i], i );
        }
        this.fieldLookup = fieldLookup;
    }

    @Override
    public void record( Value[] fields )
    {
        body.add( new SimpleRecord( fieldLookup, fields ) );
    }

    public Result build()
    {
        return new SimpleResult( fieldLookup.keySet(), body );
    }

}
