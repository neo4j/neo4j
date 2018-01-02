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
package org.neo4j.server.rest.domain;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.server.scripting.ScriptExecutor;
import org.neo4j.server.scripting.ScriptExecutorFactoryRepository;
import org.neo4j.server.scripting.javascript.JavascriptExecutor;

import static org.neo4j.graphdb.traversal.Evaluators.excludeStartPosition;

/**
 * This factory can instantiate or get {@link Evaluator}s from a description.
 * Either it returns built-in evaluators, or instantiates wrappers around
 * user-supplied scripts, f.ex. javascript.
 */
public class EvaluatorFactory
{
    private static final String BUILTIN = "builtin";
    private static final String KEY_LANGUAGE = "language";
    private static final String KEY_BODY = "body";
    private static final String KEY_NAME = "name";

    private final ScriptExecutorFactoryRepository factoryRepo;

    public EvaluatorFactory( boolean enableSandboxing )
    {
        Map<String,ScriptExecutor.Factory> languages = new HashMap<>();
        languages.put( "javascript", new JavascriptExecutor.Factory( enableSandboxing ) );

        factoryRepo = new ScriptExecutorFactoryRepository( languages );
    }

    public Evaluator pruneEvaluator( Map<String, Object> description )
    {
        if ( refersToBuiltInEvaluator( description ) )
        {
            return builtInPruneEvaluator( description );
        }
        else
        {
            return new ScriptedPruneEvaluator( getOrCreateExecutorFor(description) );
        }
    }

    public Evaluator returnFilter( Map<String, Object> description )
    {
        if ( refersToBuiltInEvaluator( description ) )
        {
            return builtInReturnFilter( description );
        }
        else
        {
            return new ScriptedReturnEvaluator( getOrCreateExecutorFor(description) );
        }
    }

    private ScriptExecutor getOrCreateExecutorFor( Map<String, Object> description )
    {
        String language = (String) description.get( KEY_LANGUAGE );
        String body = (String) description.get( KEY_BODY );

        return factoryRepo.getFactory(language).createExecutorForScript( body );
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

    private static abstract class ScriptedEvaluator
    {
        private final ScriptExecutor executor;
        private final Map<String, Object> scriptContext = new HashMap<>(1);

        ScriptedEvaluator( ScriptExecutor executor )
        {
            this.executor = executor;
        }

        protected boolean evalPosition(Path path)
        {
            scriptContext.put( "position", path );

            Object out = executor.execute( scriptContext );

            if(out instanceof Boolean)
            {
                return (Boolean)out;
            }

            throw new EvaluationException("Provided script did not return a boolean value.");
        }
    }

    private static class ScriptedPruneEvaluator extends ScriptedEvaluator implements Evaluator
    {
        ScriptedPruneEvaluator( ScriptExecutor executor )
        {
            super( executor );
        }
        
        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofContinues( !evalPosition( path ) );
        }
    }

    private static class ScriptedReturnEvaluator extends ScriptedEvaluator implements Evaluator
    {
        ScriptedReturnEvaluator( ScriptExecutor executor )
        {
            super( executor );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            return Evaluation.ofIncludes( evalPosition( path ) );
        }
    }
}
