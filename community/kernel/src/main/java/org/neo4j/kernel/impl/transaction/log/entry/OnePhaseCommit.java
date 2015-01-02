/**
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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.command.LogHandler;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions.CURRENT_LOG_ENTRY_VERSION;

public class OnePhaseCommit extends LogEntryCommit
{
    public OnePhaseCommit( long txId, long timeWritten )
    {
        this( CURRENT_LOG_ENTRY_VERSION, txId, timeWritten );
    }

    public OnePhaseCommit( byte version, long txId, long timeWritten )
    {
        super( TX_1P_COMMIT, version, txId, timeWritten, "Commit" );
    }

    @Override
    public void accept( LogHandler handler ) throws IOException
    {
        handler.onePhaseCommitEntry( this );
    }

    @Override
    public <T extends LogEntry> T as()
    {
        return (T) this;
    }
}
