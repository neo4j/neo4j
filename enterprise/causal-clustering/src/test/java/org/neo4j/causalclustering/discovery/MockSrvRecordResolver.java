/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import javax.naming.NamingException;

public class MockSrvRecordResolver extends SrvRecordResolver
{

    private final HashMap<String,List<SrvRecord>> records;

    public MockSrvRecordResolver( HashMap<String,List<SrvRecord>> records )
    {
        this.records = records;
    }

    public void addRecords( String url, Collection<SrvRecord> records )
    {
        records.forEach( r -> addRecord( url, r ) );
    }

    public synchronized void addRecord( String url, SrvRecord record )
    {
        List<SrvRecord> srvRecords = records.getOrDefault( url, new ArrayList<>() );
        srvRecords.add( record );

        if ( !records.containsKey( url ) )
        {
            records.put( url, srvRecords );
        }
    }

    @Override
    public Stream<SrvRecord> resolveSrvRecord( String url ) throws NamingException
    {
        List<SrvRecord> srvRecords = records.get( url );
        if ( srvRecords == null )
        {
            NamingException e = new NamingException( "No SRV records found" );
            e.appendRemainingComponent( url );
            throw e;
        }
        return srvRecords.stream();
    }
}
