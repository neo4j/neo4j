/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import javax.transaction.xa.Xid;

import org.neo4j.helpers.Format;

public abstract class LogEntry
{
    /* version 1 as of 2011-02-22
     * version 2 as of 2011-10-17
     */
    static final byte CURRENT_VERSION = (byte) 2;
    // empty record due to memory mapped file
    public static final byte EMPTY = (byte) 0;
    public static final byte TX_START = (byte) 1;
    public static final byte TX_PREPARE = (byte) 2;
    public static final byte COMMAND = (byte) 3;
    public static final byte DONE = (byte) 4;
    public static final byte TX_1P_COMMIT = (byte) 5;
    public static final byte TX_2P_COMMIT = (byte) 6;

    private int identifier;

    LogEntry( int identifier )
    {
        this.identifier = identifier;
    }

    public int getIdentifier()
    {
        return identifier;
    }

    public static class Start extends LogEntry
    {
        private final Xid xid;
        private final int masterId;
        private final int myId;
        private final long timeWritten;
        private long startPosition;

        Start( Xid xid, int identifier, int masterId, int myId, long startPosition, long timeWritten )
        {
            super( identifier );
            this.xid = xid;
            this.masterId = masterId;
            this.myId = myId;
            this.startPosition = startPosition;
            this.timeWritten = timeWritten;
        }

        public Xid getXid()
        {
            return xid;
        }
        
        public int getMasterId()
        {
            return masterId;
        }

        public int getLocalId()
        {
            return myId;
        }

        public long getStartPosition()
        {
            return startPosition;
        }

        void setStartPosition( long position )
        {
            this.startPosition = position;
        }
        
        public long getTimeWritten()
        {
            return timeWritten;
        }

        @Override
        public String toString()
        {
            return "Start[" + getIdentifier() + ",xid=" + xid + ",master=" + masterId + ",me=" + myId + ",time=" + timestamp( timeWritten ) + "]";
        }
    }

    static class Prepare extends LogEntry
    {
        private final long timeWritten;

        Prepare( int identifier, long timeWritten )
        {
            super( identifier );
            this.timeWritten = timeWritten;
        }
        
        public long getTimeWritten()
        {
            return timeWritten;
        }

        @Override
        public String toString()
        {
            return "Prepare[" + getIdentifier() + ", " + timestamp( timeWritten ) + "]";
        }
    }

    public static abstract class Commit extends LogEntry
    {
        private final long txId;
        private final long timeWritten;

        Commit( int identifier, long txId, long timeWritten )
        {
            super( identifier );
            this.txId = txId;
            this.timeWritten = timeWritten;
        }

        public long getTxId()
        {
            return txId;
        }
        
        public long getTimeWritten()
        {
            return timeWritten;
        }
    }

    public static class OnePhaseCommit extends Commit
    {
        OnePhaseCommit( int identifier, long txId, long timeWritten )
        {
            super( identifier, txId, timeWritten );
        }

        @Override
        public String toString()
        {
            return "1PC[" + getIdentifier() + ", txId=" + getTxId() + ", " + timestamp( getTimeWritten() ) + "]";
        }
    }

    public static class Done extends LogEntry
    {
        Done( int identifier )
        {
            super( identifier );
        }

        @Override
        public String toString()
        {
            return "Done[" + getIdentifier() + "]";
        }
    }

    public static class TwoPhaseCommit extends Commit
    {
        TwoPhaseCommit( int identifier, long txId, long timeWritten )
        {
            super( identifier, txId, timeWritten );
        }

        @Override
        public String toString()
        {
            return "2PC[" + getIdentifier() + ", txId=" + getTxId() + ", " + timestamp( getTimeWritten() ) + "]";
        }
    }

    public static class Command extends LogEntry
    {
        private final XaCommand command;

        Command( int identifier, XaCommand command )
        {
            super( identifier );
            this.command = command;
        }

        public XaCommand getXaCommand()
        {
            return command;
        }

        @Override
        public String toString()
        {
            return "Command[" + getIdentifier() + ", " + command + "]";
        }
    }

    public void setIdentifier( int newXidIdentifier )
    {
        identifier = newXidIdentifier;
    }

    public String timestamp( long timeWritten )
    {
        return Format.date( timeWritten ) + "/" + timeWritten;
    }
}
