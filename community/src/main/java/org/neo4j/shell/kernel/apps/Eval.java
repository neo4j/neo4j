/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.shell.kernel.apps;

import java.lang.reflect.Method;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

/**
 * Exposes the javax.script.ScriptEngine as a shell app. It's purely via reflection so
 * it's OK even if the script engine isn't on the classpath.
 */
public class Eval extends GraphDatabaseApp
{
    private static final String JAVAX_SCRIPT_SCRIPT_ENGINE_MANAGER = "javax.script.ScriptEngineManager";
    
    private final Object scriptEngineManager;
    private final int engineScopeValue;
    
    public Eval()
    {
        Object scriptEngineManager = null;
        int engineScopeValue = 0;
        try
        {
            Class<?> scriptEngineManagerClass = Class.forName( JAVAX_SCRIPT_SCRIPT_ENGINE_MANAGER );
            scriptEngineManager = scriptEngineManagerClass.newInstance();
            engineScopeValue = Class.forName( "javax.script.ScriptContext" ).getField( "ENGINE_SCOPE" ).getInt( null );
        }
        catch ( Exception e )
        {
            scriptEngineManager = null;
        }
        this.scriptEngineManager = scriptEngineManager;
        this.engineScopeValue = engineScopeValue;
    }
    
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
            "  ' nodes = db.getAllNodes().iterator();\n" +
            "  ' while ( nodes.hasNext() )\n" +
            "  '   out.println( \"\" + nodes.next() );\n" +
            "  '\n" +
            "So either a one-liner or type 'eval' to enter multi-line mode, where an empty line denotes the end";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        if ( scriptEngineManager == null )
        {
            out.println( "Scripting not available, make sure javax.script.* is available on " +
            		"the classpath in the JVM the shell server runs in" );
            return null;
        }
        
        String javascriptCode = parser.getLineWithoutApp();
        javascriptCode = includeImports( javascriptCode );
        Object scriptEngine = scriptEngineManager.getClass().getMethod( "getEngineByName", String.class ).invoke( scriptEngineManager, "javascript" );
        addToContext( scriptEngine,
                "db", getServer().getDb(),
                "out", out,
                "current", getCurrent( session ).asPropertyContainer() );
        Object result = scriptEngine.getClass().getMethod( "eval", String.class ).invoke( scriptEngine, javascriptCode );
        if ( result != null )
        {
            out.println( result.toString() );
        }
        return null;
    }

    private void addToContext( Object scriptEngine, Object... keyValuePairs ) throws Exception
    {
        Object context = scriptEngine.getClass().getMethod( "getContext" ).invoke( scriptEngine );
        Method setAttributeMethod = context.getClass().getMethod( "setAttribute", String.class, Object.class, Integer.TYPE );
        for ( int i = 0; i < keyValuePairs.length; i++ )
        {
            setAttributeMethod.invoke( context, keyValuePairs[i++], keyValuePairs[i], engineScopeValue );
        }
    }

    private String includeImports( String javascriptCode )
    {
        return importStatement( "org.neo4j.graphdb" ) +
               importStatement( "org.neo4j.graphdb.event" ) +
               importStatement( "org.neo4j.graphdb.index" ) +
               importStatement( "org.neo4j.graphdb.traversal" ) +
               importStatement( "org.neo4j.kernel" ) +
               javascriptCode;
    }
    
    private String importStatement( String thePackage )
    {
        return "importPackage(Packages." + thePackage + ");";
    }
}
