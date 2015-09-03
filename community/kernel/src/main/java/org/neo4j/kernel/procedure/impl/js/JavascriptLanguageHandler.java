/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.procedure.impl.js;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.ScriptFunction;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleScriptContext;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.procedure.CompiledProcedure;
import org.neo4j.kernel.procedure.LanguageHandler;

public class JavascriptLanguageHandler implements LanguageHandler
{
    private final ScriptEngine engine;
    private final JavascriptProcedureBoilerplate procedureBoilerplate = new JavascriptProcedureBoilerplate();

    public JavascriptLanguageHandler()
    {
        this.engine = new ScriptEngineManager().getEngineByName( "nashorn" );
    }

    @Override
    public CompiledProcedure compile( ProcedureSource source ) throws ProcedureException
    {
        try
        {
            // Init procedure-local context
            ScriptContext ctx = new SimpleScriptContext();

            // Wrap user code in boilerplate signature
            String code = procedureBoilerplate.wrapAsProcedureFunction( source );

            // Compile the ES5 generator function
            ScriptFunction procedureFunc = (ScriptFunction) NashornUtil.unwrap( (ScriptObjectMirror) engine.eval( code, ctx ) );

            return new JavascriptCompiledProcedure( ctx, procedureFunc, new JavascriptTypeMapper(source.signature()), source.signature() );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Statement.ProcedureError, e, "Failed to compile javascript procedure `%s`. Error was: %n%s", source.signature(), e.getMessage() );
        }
    }
}
