/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.restore;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;

public class RestoreClusterUtils
{
    public static StringBuilder execute( Runnable function )
    {
        StringBuilder builder = new StringBuilder();
        PrintStream theRealOut = System.out;
        System.setOut( new PrintStream( new MyOutputStream( builder ) ) );
        function.run();
        System.setOut( theRealOut );
        return builder;
    }

    public static StringBuilder execute( AdminCommand command, String[] args )
    {
        return execute( () -> {
            try
            {
                command.execute( args );
            }
            catch ( IncorrectUsage | CommandFailed incorrectUsage )
            {
                throw new RuntimeException( incorrectUsage );
            }
        });
    }

    private static class MyOutputStream extends OutputStream
    {
        private final StringBuilder stringBuilder;

        public MyOutputStream( StringBuilder stringBuilder )
        {
            this.stringBuilder = stringBuilder;
        }

        @Override
        public void write( int b ) throws IOException
        {
            stringBuilder.append( (char) b );
        }
    }

}
