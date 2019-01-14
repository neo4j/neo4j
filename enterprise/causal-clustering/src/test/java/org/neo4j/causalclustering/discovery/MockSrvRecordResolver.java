/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
