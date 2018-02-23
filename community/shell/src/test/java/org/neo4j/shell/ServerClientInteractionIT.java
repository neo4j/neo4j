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
package org.neo4j.shell;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.regex.Pattern.compile;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.shell.Variables.PROMPT_KEY;

public class ServerClientInteractionIT extends AbstractShellIT
{
    private SilentLocalOutput out = new SilentLocalOutput();

    @BeforeEach
    public void setup()
    {
        makeServerRemotelyAvailable();
    }

    @Test
    public void shouldConsiderAndInterpretCustomClientPrompt() throws Exception
    {
        // GIVEN
        shellClient.setSessionVariable( PROMPT_KEY, "MyPrompt \\d \\t$ " );

        // WHEN
        Response response = shellServer.interpretLine( shellClient.getId(), "", out );

        // THEN
        String regexPattern = "MyPrompt .{1,3} .{1,3} \\d{1,2} \\d{2}:\\d{2}:\\d{2}\\$";
        assertTrue( compile( regexPattern ).matcher( response.getPrompt() ).find(),
                "Prompt from server '" + response.getPrompt() + "' didn't match pattern '" + regexPattern + "'" );
    }

}
