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
import java.io.IOException;

import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.replication.RaftContentSerializer;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.monitoring.Monitors;

public class ReplayRaftLog
{
    public static void main( String[] args ) throws RaftStorageException, IOException
    {
        Args arg = Args.parse( args );

        String from = arg.get( "from" );
        System.out.println("From is " + from);
        String to = arg.get( "to" );
        System.out.println("to is " + to);


        File logDirectory = new File( from );
        System.out.println( "logDirectory = " + logDirectory );
        NaiveDurableRaftLog log = new NaiveDurableRaftLog( new DefaultFileSystemAbstraction(),
                logDirectory, new RaftContentSerializer(), new Monitors() );

        long totalCommittedEntries = log.commitIndex();


//        File target = new File( to );
//        RandomAccessFile newLog = new RandomAccessFile( target, "rw" );

        for ( int i = 0; i <= totalCommittedEntries; i++ )
        {
            ReplicatedContent content = log.readLogEntry( i ).content();
            if ( content instanceof ReplicatedTransaction )
            {
                ReplicatedTransaction tx = (ReplicatedTransaction) content;
                ReplicatedTransactionFactory.extractTransactionRepresentation( tx, new byte[0] ).accept(
                        new Visitor<Command, IOException>()
                        {
                            @Override
                            public boolean visit( Command element ) throws IOException
                            {
                                System.out.println(element);
                                return false;
                            }
                        } );
            }
        }
//        newLog.close();
    }
}
