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
import org.jline.reader.LineReader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.parser.StatementParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class JlineCompleterTest
{
    private final JlineCompleter completer = new JlineCompleter( new CommandHelper.CommandFactoryHelper() );
    private final LineReader lineReader = mock( LineReader.class );

    @Test
    void completeCommands()
    {
        var candidates = new ArrayList<Candidate>();
        var line = new StatementJlineParser.CompletingCommand( new StatementParser.CommandStatement( ":", List.of() ), ":", 1 );
        completer.complete( lineReader, line, candidates );

        var values = candidates.stream().map( Candidate::value ).toList();
        assertThat( values, is( List.of(
                ":begin",
                ":commit",
                ":connect",
                ":disconnect",
                ":exit",
                ":help",
                ":history",
                ":log",
                ":param",
                ":params",
                ":rollback",
                ":source",
                ":use"
        ) ) );
    }
}
