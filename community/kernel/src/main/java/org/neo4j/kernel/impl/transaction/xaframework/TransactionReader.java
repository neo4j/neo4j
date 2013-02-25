/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.nioneo.xa.CommandRecordVisitor;

/**
 * This class is for single threaded use only.
 *
 * @author Tobias Lindaaker
 */
public class TransactionReader
{
    public interface Visitor
    {
        void visitStart( int localId, byte[] globalTransactionId, int masterId, int myId, long startTimestamp );

        void visitPrepare( int localId, long prepareTimestamp );

        void visitCommit( int localId, boolean twoPhase, long txId, long commitTimestamp );

        void visitDone( int localId );

        void visitUpdateNode( int localId, NodeRecord node );

        void visitDeleteNode( int localId, long node );

        void visitUpdateRelationship( int localId, RelationshipRecord node );

        void visitDeleteRelationship( int localId, long node );

        void visitUpdateProperty( int localId, PropertyRecord node );

        void visitDeleteProperty( int localId, long node );

        void visitUpdateRelationshipType( int localId, RelationshipTypeRecord node );

        void visitDeleteRelationshipType( int localId, int node );

        void visitUpdatePropertyIndex( int localId, PropertyIndexRecord node );

        void visitDeletePropertyIndex( int localId, int node );

        void visitUpdateNeoStore( int localId, NeoStoreRecord node );

        void visitDeleteNeoStore( int localId, long node );
    }

    private static final XaCommandFactory COMMAND_FACTORY = new XaCommandFactory()
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, null, byteChannel, buffer );
        }
    };
    private final ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );

    public void read( ReadableByteChannel source, Visitor visitor ) throws IOException
    {
        for ( LogEntry entry; null != (entry = readEntry( source )); )
        {
            if ( entry instanceof LogEntry.Command )
            {
                Command command = (Command) ((LogEntry.Command) entry).getXaCommand();
                command.accept( new CommandVisitor( entry.getIdentifier(), visitor ) );
            }
            else if ( entry instanceof LogEntry.Start )
            {
                LogEntry.Start start = (LogEntry.Start) entry;
                visitor.visitStart( start.getIdentifier(), start.getXid().getGlobalTransactionId(), start.getMasterId(),
                                    start.getLocalId(), start.getTimeWritten() );
            }
            else if ( entry instanceof LogEntry.Prepare )
            {
                LogEntry.Prepare prepare = (LogEntry.Prepare) entry;
                visitor.visitPrepare( prepare.getIdentifier(), prepare.getTimeWritten() );
            }
            else if ( entry instanceof LogEntry.OnePhaseCommit )
            {
                LogEntry.OnePhaseCommit commit = (LogEntry.OnePhaseCommit) entry;
                visitor.visitCommit( commit.getIdentifier(), false, commit.getTxId(), commit.getTimeWritten() );
            }
            else if ( entry instanceof LogEntry.TwoPhaseCommit )
            {
                LogEntry.TwoPhaseCommit commit = (LogEntry.TwoPhaseCommit) entry;
                visitor.visitCommit( commit.getIdentifier(), true, commit.getTxId(), commit.getTimeWritten() );
            }
            else if ( entry instanceof LogEntry.Done )
            {
                LogEntry.Done done = (LogEntry.Done) entry;
                visitor.visitDone( done.getIdentifier() );
            }
        }
    }

    private LogEntry readEntry( ReadableByteChannel source ) throws IOException
    {
        return LogIoUtils.readEntry( buffer, source, COMMAND_FACTORY );
    }

    private static class CommandVisitor implements CommandRecordVisitor
    {
        private final int localId;
        private final Visitor visitor;

        public CommandVisitor( int localId, Visitor visitor )
        {
            this.localId = localId;
            this.visitor = visitor;
        }

        @Override
        public void visitNode( NodeRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeleteNode( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdateNode( localId, record );
            }
        }

        @Override
        public void visitRelationship( RelationshipRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeleteRelationship( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdateRelationship( localId, record );
            }
        }

        @Override
        public void visitProperty( PropertyRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeleteProperty( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdateProperty( localId, record );
            }
        }

        @Override
        public void visitRelationshipType( RelationshipTypeRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeleteRelationshipType( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdateRelationshipType( localId, record );
            }
        }

        @Override
        public void visitPropertyIndex( PropertyIndexRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeletePropertyIndex( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdatePropertyIndex( localId, record );
            }
        }

        @Override
        public void visitNeoStore( NeoStoreRecord record )
        {
            if ( !record.inUse() )
            {
                visitor.visitDeleteNeoStore( localId, record.getId() );
            }
            else
            {
                visitor.visitUpdateNeoStore( localId, record );
            }
        }
    }
}
