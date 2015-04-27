/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.entry.*;

public interface LogHandler
{
    void startLog();

    void startEntry( LogEntryStart startEntry ) throws IOException;

    void onePhaseCommitEntry( OnePhaseCommit onePhaseCommitEntry ) throws IOException;

    void commandEntry( LogEntryCommand commandEntry ) throws IOException;

    void endLog( boolean success ) throws IOException;

    public abstract class Filter implements LogHandler
    {
        protected LogHandler delegate;

        public Filter( LogHandler delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void startLog()
        {
            delegate.startLog();
        }

        @Override
        public void startEntry( LogEntryStart startEntry ) throws IOException
        {
            delegate.startEntry( startEntry );
        }

        @Override
        public void onePhaseCommitEntry( OnePhaseCommit onePhaseCommitEntry ) throws IOException
        {
            delegate.onePhaseCommitEntry( onePhaseCommitEntry );
        }

        @Override
        public void commandEntry( LogEntryCommand commandEntry ) throws IOException
        {
            delegate.commandEntry( commandEntry );
        }

        @Override
        public void endLog( boolean success ) throws IOException
        {
            delegate.endLog( true );
        }
    }
}
