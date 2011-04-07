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

public abstract class LogEntry
{
    // version 1 as of 2011-02-22
    static final byte CURRENT_VERSION = (byte) 1;
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
        private long startPosition;

        Start( Xid xid, int identifier, long startPosition )
        {
            super( identifier );
            this.xid = xid;
            this.startPosition = startPosition;
        }

        public Xid getXid()
        {
            return xid;
        }

        public long getStartPosition()
        {
            return startPosition;
        }

        void setStartPosition( long position )
        {
            this.startPosition = position;
        }

        @Override
        public String toString()
        {
            return "Start[" + getIdentifier() + "]";
        }
    }

    static class Prepare extends LogEntry
    {
        Prepare( int identifier )
        {
            super( identifier );
        }

        @Override
        public String toString()
        {
            return "Prepare[" + getIdentifier() + "]";
        }
    }

    public static abstract class Commit extends LogEntry
    {
        private final long txId;
        private final int masterId;

        Commit( int identifier, long txId, int masterId )
        {
            super( identifier );
            this.txId = txId;
            this.masterId = masterId;
        }

        public long getTxId()
        {
            return txId;
        }

        public int getMasterId()
        {
            return masterId;
        }
    }

    public static class OnePhaseCommit extends Commit
    {
        OnePhaseCommit( int identifier, long txId, int masterId )
        {
            super( identifier, txId, masterId );
        }

        @Override
        public String toString()
        {
            return "1PC[" + getIdentifier() + ", txId=" + getTxId() + ", masterId=" + getMasterId() + "]";
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
        TwoPhaseCommit( int identifier, long txId, int masterId )
        {
            super( identifier, txId, masterId );
        }

        @Override
        public String toString()
        {
            return "2PC[" + getIdentifier() + ", txId=" + getTxId() + ", machineId=" + getMasterId() + "]";
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
}
