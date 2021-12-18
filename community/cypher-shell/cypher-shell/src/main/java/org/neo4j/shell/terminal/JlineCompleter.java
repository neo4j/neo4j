/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.terminal;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.List;

import org.neo4j.shell.commands.Command;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.terminal.StatementJlineParser.CompletingCommand;

/**
 * Provides auto completion for (some) cypher shell statements.
 */
public class JlineCompleter implements Completer
{
    private final List<Candidate> allCommands;

    public JlineCompleter( CommandFactoryHelper commands )
    {
        this.allCommands = commands.factories().stream().map( f -> toCandidate( f.metadata() ) ).sorted().toList();
    }

    private static Candidate toCandidate( Command.Metadata command )
    {
        return new Candidate( command.name(), command.name(), "Command", command.description(), null, null, true );
    }

    @Override
    public void complete( LineReader reader, ParsedLine line, List<Candidate> candidates )
    {
        // Complete Cypher Shell commands
        if ( line instanceof CompletingCommand )
        {
            candidates.addAll( allCommands );
        }
    }
}
