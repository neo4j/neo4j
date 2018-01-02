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
package org.neo4j.server.scripting.javascript;

import java.util.Map;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.ImporterTopLevel;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Undefined;
import org.neo4j.server.rest.domain.EvaluationException;
import org.neo4j.server.scripting.ScriptExecutor;

public class JavascriptExecutor implements ScriptExecutor
{
    private final Scriptable prototype;
    private final Script compiledScript;

    public static class Factory implements ScriptExecutor.Factory
    {
        /**
         * Note that you can set sandbox/no sandbox once, after that it is globally defined
         * for the JVM. If you create a new factory with a different sandboxing setting, an
         * exception will be thrown.
         *
         * @param enableSandboxing
         */
        public Factory(boolean enableSandboxing)
        {
            if(enableSandboxing)
            {
                GlobalJavascriptInitializer.initialize( GlobalJavascriptInitializer.Mode.SANDBOXED );
            } else
            {
                GlobalJavascriptInitializer.initialize( GlobalJavascriptInitializer.Mode.UNSAFE );
            }
        }

        @Override
        public ScriptExecutor createExecutorForScript( String script ) throws EvaluationException
        {
            return new JavascriptExecutor( script );
        }
    }

    public JavascriptExecutor( String script )
    {
        Context cx = Context.enter();
        try
        {
            prototype = createPrototype(cx);
            compiledScript = cx.compileString( script, "Unknown", 0, null );
        } finally
        {
            Context.exit();
        }
    }

    private Scriptable createPrototype( Context cx )
    {
        Scriptable proto = cx.initStandardObjects();
        Scriptable topLevel = new ImporterTopLevel(cx);
        proto.setParentScope( topLevel );

        return proto;
    }

    @Override
    public Object execute( Map<String, Object> variables ) throws EvaluationException
    {
        Context cx = Context.enter();
        try
        {
            Scriptable scope = cx.newObject(prototype);
            scope.setPrototype(prototype);

            if(variables != null)
            {
                for(String k : variables.keySet())
                {
                    scope.put( k, scope, variables.get( k ) );
                }
            }

            Object out = compiledScript.exec( cx, scope );
            if(out instanceof NativeJavaObject)
            {
                return ((NativeJavaObject)out).unwrap();
            } else if(out instanceof Undefined )
            {
                return null;
            } else
            {
                return out;
            }
        } catch( RhinoException e )
        {
            throw new EvaluationException( "Failed to execute script, see nested exception.", e );
        } finally
        {
            Context.exit();
        }
    }
}
