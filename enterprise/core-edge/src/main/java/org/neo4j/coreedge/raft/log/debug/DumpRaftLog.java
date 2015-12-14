/*
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
package org.neo4j.coreedge.raft.log.debug;

import java.io.File;

import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.replication.RaftContentSerializer;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;

public class DumpRaftLog
{
    public static void main( String[] args ) throws RaftStorageException
    {
        for ( String arg : args )
        {
            File logDirectory = new File( arg );
            System.out.println( "logDirectory = " + logDirectory );
            NaiveDurableRaftLog log = new NaiveDurableRaftLog( new DefaultFileSystemAbstraction(),
                    logDirectory, new RaftContentSerializer(), new Monitors() );

            new LogPrinter( log ).print( System.out );
            System.out.println();
        }
    }
}
