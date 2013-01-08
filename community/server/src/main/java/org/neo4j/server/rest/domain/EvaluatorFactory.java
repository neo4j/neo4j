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
package org.neo4j.server.rest.domain;

import static org.neo4j.graphdb.traversal.Evaluators.excludeStartPosition;

import java.util.Map;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

/**
<<<<<<< HEAD
 * This factory can instantiate or get {@link Evaluator}s from a description.
 * Either it returns built-in evaluators, or instantiates wrappers around
 * user-supplied scripts, f.ex. javascript.
=======
 * This factory can instantiate or get {@link PruneEvaluator}s and
 * {@link ReturnFilter}s from a description. Either it returns built-in
 * evaluators, or instantiates wrappers around user-supplied scripts, f.ex.
 * javascript.
>>>>>>> master
 */
abstract class EvaluatorFactory
{
    private static final String BUILTIN = "builtin";
    private static final String KEY_LANGUAGE = "language";
    private static final String KEY_BODY = "body";
    private static final String KEY_NAME = "name";

    public static Evaluator pruneEvaluator( Map<String, Object> description )
    {
        if ( refersToBuiltInEvaluator( description ) )
        {
            return builtInPruneEvaluator( description );
        }
        else
        {
            return new ScriptedPruneEvaluator( scriptEngine( description ), (String) description.get( KEY_BODY ) );
        }
    }

    public static Evaluator returnFilter( Map<String, Object> description )
    {
        if ( refersToBuiltInEvaluator( description ) )
        {
            return builtInReturnFilter( description );
        }
        else
        {
            return new ScriptedReturnEvaluator( scriptEngine( description ), (String) description.get( KEY_BODY ) );
        }
    }

    private static boolean refersToBuiltInEvaluator( Map<String, Object> description )
    {
        String language = (String) description.get( KEY_LANGUAGE );
        return language.equals( BUILTIN );
    }

    private static Evaluator builtInPruneEvaluator( Map<String, Object> description )
    {
        String name = (String) description.get( KEY_NAME );
        // FIXME I don't like these hardcoded strings
        if ( name.equalsIgnoreCase( "none" ) )
        {
            return null;
        }
        else
        {
            throw new EvaluationException( "Unrecognized prune evaluator name '" + name + "'" );
        }
    }

    private static Evaluator builtInReturnFilter( Map<String, Object> description )
    {
        String name = (String) description.get( KEY_NAME );
        // FIXME I don't like these hardcoded strings
        if ( name.equalsIgnoreCase( "all" ) )
        {
            return null;
        }
        else if ( name.equalsIgnoreCase( "all_but_start_node" ) )
        {
            return excludeStartPosition();
        }
        else
        {
            throw new EvaluationException( "Unrecognized return evaluator name '" + name + "'" );
        }
    }

    private static ScriptEngine scriptEngine( Map<String, Object> description )
    {
        String language = (String) description.get( KEY_LANGUAGE );
        ScriptEngine engine = new ScriptEngineManager().getEngineByName( language );
        if ( engine == null )
        {
            throw new EvaluationException( "Unknown script language '" + language + "'" );
        }
        return engine;
    }

    /**
     * An abstract for {@link ScriptEngine} and {@link CompiledScript}. They
     * have no common interface... other than this one.
     */
    private static abstract class ScriptExecutor
    {
        abstract Object eval( Path position );
    }

    private static class EvalScriptExecutor extends ScriptExecutor
    {
        private final ScriptEngine script;
        private final String body;

        EvalScriptExecutor( ScriptEngine script, String body )
        {
            this.script = script;
            this.body = body;
        }

        @Override
        Object eval( Path position )
        {
            try
            {
                this.script.getContext()
                        .setAttribute( "position", position, ScriptContext.ENGINE_SCOPE );
                return this.script.eval( body );
            }
            catch ( ScriptException e )
            {
                throw new EvaluationException( e );
            }
        }
    }

    private static class CompiledScriptExecutor extends ScriptExecutor
    {
        private final CompiledScript script;
        private final ScriptContext context;

        CompiledScriptExecutor( CompiledScript script, ScriptContext context )
        {
            this.script = script;
            this.context = context;
        }

        @Override
        Object eval( Path position )
        {
            try
            {
                this.context.setAttribute( "position", position, ScriptContext.ENGINE_SCOPE );
                return this.script.eval( this.context );
            }
            catch ( ScriptException e )
            {
                throw new EvaluationException( e );
            }
        }
    }

    private static abstract class ScriptedEvaluator
    {
        private final ScriptEngine engine;
        private final String body;
        private ScriptExecutor executor;

        ScriptedEvaluator( ScriptEngine engine, String body )
        {
            this.engine = engine;
            this.body = body;
        }

        protected ScriptExecutor executor( Path position )
        {
            // We'll have to decide between evaluated script or compiled script
            // the first time we execute it, else the compiled script can't be
            // compiled (since position must be a valid object).
            if ( this.executor == null )
            {
                try
                {
                    ScriptContext context = new SimpleScriptContext();
                    context.setAttribute( "position", position, ScriptContext.ENGINE_SCOPE );
                    this.engine.setContext( context );
                    if ( this.engine instanceof Compilable )
                    {
                        this.executor = new CompiledScriptExecutor( ( (Compilable) engine ).compile( body ), context );
                    }
                    else
                    {
                        this.executor = new EvalScriptExecutor( engine, body );
                    }
                    return executor;
                }
                catch ( ScriptException e )
                {
                    throw new EvaluationException( e );
                }
            }
            return this.executor;
        }
    }

    private static class ScriptedPruneEvaluator extends ScriptedEvaluator implements Evaluator
    {
        ScriptedPruneEvaluator( ScriptEngine engine, String body )
        {
            super( engine, body );
        }
        
        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofContinues( !(Boolean)executor( path ).eval( path ) );
        }
    }

    private static class ScriptedReturnEvaluator extends ScriptedEvaluator implements Evaluator
    {
        ScriptedReturnEvaluator( ScriptEngine engine, String body )
        {
            super( engine, body );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofIncludes( (Boolean) this.executor( path ).eval( path ) );
        }
    }
}
