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
package org.neo4j.shell.cli;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import java.util.Map;

import org.neo4j.shell.log.Logger;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

/**
 * Action that parses and appends query parameters.
 */
public class LogLevelArgumentAction implements ArgumentAction
{
    @Override
    public void run( ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value ) throws ArgumentParserException
    {
        try
        {
            attrs.put( arg.getDest(), Logger.Level.from( value.toString() ) );
        }
        catch ( Exception e )
        {
            throw new ArgumentParserException( "Incorrect usage.\n" + usage(), parser );
        }
    }

    static String usage()
    {
        var levels = stream( Logger.Level.values() ).map( l -> l.name().toLowerCase() ).collect( joining( ", " ) );
        return "Usage: `--log` to log everything, or `--log <level>` where level is one of " + levels + ".";
    }

    @Override
    public void onAttach( Argument arg )
    {

    }

    @Override
    public boolean consumeArgument()
    {
        return true;
    }
}
