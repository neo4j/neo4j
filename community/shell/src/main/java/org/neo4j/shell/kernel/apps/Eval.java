/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.shell.kernel.apps;

import static org.neo4j.shell.kernel.apps.ScriptEngineViaReflection.decorateWithImports;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

/**
 * Exposes the javax.script.ScriptEngine as a shell app. It's purely via reflection so
 * it's OK even if the script engine isn't on the classpath.
 */
public class Eval extends TransactionProvidingApp
{
    private ScriptEngineViaReflection scripting;
    
    @Override
    public String getDescription()
    {
        return "Pass JavaScript to be executed on the shell server, directly on the database. " +
            "There are predefined variables you can use:\n" +
            "  db      : the GraphDatabaseService on the server\n" +
            "  out     : output back to you (the shell client)\n" +
            "  current : current node or relationship you stand on\n\n" +
            "Usage:\n" +
            "  eval db.getReferenceNode().getProperty(\"name\")\n" +
            "  \n" +
            "  eval\n" +
            "  > nodes = db.getAllNodes().iterator();\n" +
            "  > while ( nodes.hasNext() )\n" +
            "  >   out.println( \"\" + nodes.next() );\n" +
            "  >\n" +
            "So either a one-liner or type 'eval' to enter multi-line mode, where an empty line denotes the end";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        // satisfied if:
        // * it ends with \n
        // * there's stuff after eval and no \n on the line
        boolean satisfied =
                parser.getLine().endsWith( "\n" ) ||
                (parser.getLineWithoutApp().length() > 0 && parser.getLine().indexOf( '\n' ) == -1);
        if ( !satisfied ) return Continuation.INPUT_INCOMPLETE;
        scripting = scripting != null ? scripting : new ScriptEngineViaReflection( getServer() );
        String javascriptCode = parser.getLineWithoutApp();
        javascriptCode = decorateWithImports( javascriptCode, STANDARD_EVAL_IMPORTS );
        Object scriptEngine = scripting.getJavascriptEngine();
        scripting.addDefaultContext( scriptEngine, session, out );
        Object result = scripting.interpret( scriptEngine, javascriptCode );
        if ( result != null )
        {
            out.println( result.toString() );
        }
        return Continuation.INPUT_COMPLETE;
    }
}
