/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

final class IndexCommandDetector extends CommandHandler.Delegator
{
    private boolean hasWrittenAnyLegacyIndexCommand;

    public IndexCommandDetector( CommandHandler delegate )
    {
        super( delegate );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        // If there's any legacy index command in this transaction, there's an index define command
        // so it's enough to check this command type.
        hasWrittenAnyLegacyIndexCommand = true;
        return super.visitIndexDefineCommand( command );
    }

    public void reset()
    {
        hasWrittenAnyLegacyIndexCommand = false;
    }

    public boolean hasWrittenAnyLegacyIndexCommand()
    {
        return hasWrittenAnyLegacyIndexCommand;
    }
}
