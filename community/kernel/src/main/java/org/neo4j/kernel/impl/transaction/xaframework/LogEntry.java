/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.Arrays;
import java.util.TimeZone;

import org.neo4j.helpers.Format;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;

public abstract class LogEntry
{
    public static final int MASTER_ID_REPRESENTING_NO_MASTER = -1;

    /* version 1 as of 2011-02-22
     * version 2 as of 2011-10-17
     * version 3 as of 2013-02-09: neo4j 2.0 Labels & Indexing
     * version 4 as of 2014-02-06: neo4j 2.1 Dense nodes, split by type/direction into groups
     */
    public static final byte CURRENT_LOG_VERSION = (byte) 4;

    /*
     * version 0 for Neo4j versions < 2.1
     * version -1 for Neo4j 2.1
     */
    public static final byte CURRENT_LOG_ENTRY_VERSION = (byte) -1;

    // empty record due to memory mapped file
    public static final byte EMPTY = (byte) 0;

    // Real entries
    public static final byte TX_START = (byte) 1;
    public static final byte COMMAND = (byte) 3;
    public static final byte TX_1P_COMMIT = (byte) 5;

    private final byte type;
    private final byte version;

    LogEntry( byte type, byte version )
    {
        this.type = type;
        this.version = version;
    }

    public abstract void accept( LogHandler handler ) throws IOException;

    public byte getType()
    {
        return type;
    }

    public byte getVersion()
    {
        return version;
    }

    public String toString( TimeZone timeZone )
    {
        return toString();
    }

    public static class
            Start extends LogEntry
    {
        private final int masterId;
        private final int myId;
        private final long timeWritten;
        private final long lastCommittedTxWhenTransactionStarted;
        private final byte[] additionalHeader;
        private LogPosition startPosition;

        public Start( int masterId, int myId, long timeWritten,
                      long lastCommittedTxWhenTransactionStarted, byte[] additionalHeader,
                      LogPosition startPosition )
        {
            this( CURRENT_LOG_ENTRY_VERSION, masterId, myId, timeWritten,
                    lastCommittedTxWhenTransactionStarted, additionalHeader, startPosition );
        }

        public Start( byte version, int masterId, int myId, long timeWritten,
               long lastCommittedTxWhenTransactionStarted, byte[] additionalHeader, LogPosition startPosition )
        {
            super( TX_START, version );
            this.masterId = masterId;
            this.myId = myId;
            this.startPosition = startPosition;
            this.timeWritten = timeWritten;
            this.lastCommittedTxWhenTransactionStarted = lastCommittedTxWhenTransactionStarted;
            this.additionalHeader = additionalHeader;
        }

        public int getMasterId()
        {
            return masterId;
        }

        public int getLocalId()
        {
            return myId;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
        }

        public void setStartPosition( LogPosition position )
        {
            this.startPosition = position;
        }

        public long getTimeWritten()
        {
            return timeWritten;
        }

        public long getLastCommittedTxWhenTransactionStarted()
        {
            return lastCommittedTxWhenTransactionStarted;
        }

        public byte[] getAdditionalHeader()
        {
            return additionalHeader;
        }

        /**
         * @return combines necessary state to get a unique checksum to identify this transaction uniquely.
         */
        public long getChecksum()
        {
            // [4 bits combined masterId/myId][4 bits xid hashcode, which combines time/randomness]
            long lowBits = Arrays.hashCode( additionalHeader );
            long highBits = masterId*37 + myId;
            return (highBits << 32) | (lowBits & 0xFFFFFFFFL);
        }

        @Override
        public String toString()
        {
            return toString( Format.DEFAULT_TIME_ZONE );
        }

        @Override
        public void accept( LogHandler handler ) throws IOException
        {
            handler.startEntry( this );
        }

        @Override
        public String toString( TimeZone timeZone )
        {
            // TODO 2.2-future include additionalHeader in toString?
            return "Start[master=" + masterId + ",me=" + myId + ",time=" +
                    timestamp( timeWritten, timeZone ) + ",lastCommittedTxWhenTransactionStarted="+
                    lastCommittedTxWhenTransactionStarted+"]";
        }
    }

    public static abstract class Commit extends LogEntry
    {
        private final long txId;
        private final long timeWritten;
        protected final String name;

        Commit( byte type, byte version, long txId, long timeWritten, String name )
        {
            super( type, version );
            this.txId = txId;
            this.timeWritten = timeWritten;
            this.name = name;
        }

        public long getTxId()
        {
            return txId;
        }

        public long getTimeWritten()
        {
            return timeWritten;
        }

        @Override
        public String toString()
        {
            return toString( Format.DEFAULT_TIME_ZONE );
        }

        @Override
        public String toString( TimeZone timeZone )
        {
            return name + "[txId=" + getTxId() + ", " + timestamp( getTimeWritten(), timeZone ) + "]";
        }
    }

    public static class OnePhaseCommit extends Commit
    {
        public OnePhaseCommit( long txId, long timeWritten )
        {
            this( CURRENT_LOG_ENTRY_VERSION, txId, timeWritten );
        }

        public OnePhaseCommit( byte version, long txId, long timeWritten )
        {
            super( TX_1P_COMMIT, version, txId, timeWritten, "1PC" );
        }

        @Override
        public void accept( LogHandler handler ) throws IOException
        {
            handler.onePhaseCommitEntry( this );
        }
    }

    public static class Command extends LogEntry
    {
        private final org.neo4j.kernel.impl.nioneo.xa.command.Command command;

        public Command( org.neo4j.kernel.impl.nioneo.xa.command.Command command )
        {
            this( CURRENT_LOG_ENTRY_VERSION, command );
        }

        public Command( byte version, org.neo4j.kernel.impl.nioneo.xa.command.Command command )
        {
            super( COMMAND, version );
            this.command = command;
        }

        public org.neo4j.kernel.impl.nioneo.xa.command.Command getXaCommand()
        {
            return command;
        }

        @Override
        public String toString()
        {
            return "Command[" + command + "]";
        }

        @Override
        public void accept( LogHandler handler ) throws IOException
        {
            handler.commandEntry( this );
        }
    }

    public String timestamp( long timeWritten, TimeZone timeZone )
    {
        return Format.date( timeWritten, timeZone ) + "/" + timeWritten;
    }
}
