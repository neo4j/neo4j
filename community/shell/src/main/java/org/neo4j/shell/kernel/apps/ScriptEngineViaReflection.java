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

import java.lang.reflect.Method;

import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static org.neo4j.shell.kernel.apps.TransactionProvidingApp.getCurrent;

public class ScriptEngineViaReflection
{
    private static final String JAVAX_SCRIPT_SCRIPT_ENGINE_MANAGER = "javax.script.ScriptEngineManager";
    private static final String JAVAX_SCRIPT_SCRIPT_CONTEXT = "javax.script.ScriptContext";
    
    private final Object scriptEngineManager;
    private final int engineScopeValue;
    private final GraphDatabaseShellServer server;

    public ScriptEngineViaReflection( GraphDatabaseShellServer server )
    {
        this.server = server;
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
    
    public Object getJavascriptEngine() throws Exception
    {
        return getEngineByName( "javascript" );
    }
    
    public Object getEngineByName( String name ) throws Exception
    {
        if ( scriptEngineManager == null )
        {
            throw new ShellException( "Scripting not available, make sure javax.script.* is available on " +
                    "the classpath in the JVM the shell server runs in" );
        }
        return scriptEngineManager.getClass().getMethod( "getEngineByName", String.class ).invoke( scriptEngineManager, name );
    }
    
    public Object interpret( Object scriptEngine, String code ) throws Exception
    {
        return scriptEngine.getClass().getMethod( "eval", String.class ).invoke( scriptEngine, code );
    }

    public void addDefaultContext( Object scriptEngine, Session session, Output out )
            throws Exception, ShellException
    {
        addToContext( scriptEngine,
                "db", server.getDb(),
                "out", out,
                "current", session.getCurrent() == null ? null : getCurrent( server, session ).asPropertyContainer() );
    }
    
    public Object compile( Object scriptEngine, String code ) throws Exception
    {
        return scriptEngine.getClass().getMethod( "compile", String.class ).invoke( scriptEngine, code );
    }
    
    public static String decorateWithImports( String code, String... imports )
    {
        String importCode = "";
        for ( String oneImport : imports )
        {
            importCode += importStatement( oneImport );
        }
        return importCode + code;
    }
    
    public Object newContext() throws Exception
    {
        return Class.forName( "javax.script.SimpleScriptContext" ).getConstructor().newInstance();
    }
    
    public Object executeCompiledScript( Object compiledScript, Object context ) throws Exception
    {
        Method method = compiledScript.getClass().getMethod( "eval", Class.forName( JAVAX_SCRIPT_SCRIPT_CONTEXT ) );
        method.setAccessible( true );
        return method.invoke( compiledScript, context );
    }

    public void addToContext( Object scriptEngine, Object... keyValuePairs ) throws Exception
    {
        Object context = scriptEngine.getClass().getMethod( "getContext" ).invoke( scriptEngine );
        Method setAttributeMethod = context.getClass().getMethod( "setAttribute", String.class, Object.class, Integer.TYPE );
        for ( int i = 0; i < keyValuePairs.length; i++ )
        {
            setAttributeMethod.invoke( context, keyValuePairs[i++], keyValuePairs[i], engineScopeValue );
        }
    }
    
    public void setContextAttribute( Object context, String key, Object value ) throws Exception
    {
        Method setAttributeMethod = context.getClass().getMethod( "setAttribute", String.class,
                Object.class, Integer.TYPE );
        setAttributeMethod.invoke( context, key, value, engineScopeValue );
    }
    
    private static String importStatement( String thePackage )
    {
        return "importPackage(Packages." + thePackage + ");";
    }
}
