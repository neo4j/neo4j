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

import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.Context;
import jdk.nashorn.internal.runtime.ScriptFunction;
import jdk.nashorn.internal.runtime.ScriptObject;
import jdk.nashorn.internal.runtime.ScriptRuntime;

import java.util.ArrayList;
import java.util.List;
import javax.script.ScriptContext;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.procedure.CompiledProcedure;

public class JavascriptCompiledProcedure implements CompiledProcedure
{
    /** Map types in and out of javascript, this is tied to {@link #signature} */
    private final JavascriptTypeMapper mapper;

    /** The public procedure signature we abide by */
    private final ProcedureSignature signature;

    /** The context we execute in (eg. access to global javascript vars etc.) */
    private final ScriptContext ctx;

    /** The compiled user code - a javascript function that yields a generator */
    private final ScriptFunction procedureFunc;

    public JavascriptCompiledProcedure( ScriptContext ctx, ScriptFunction procedureFunc, JavascriptTypeMapper mapper, ProcedureSignature signature )
    {
        this.ctx = ctx;
        this.procedureFunc = procedureFunc;
        this.mapper = mapper;
        this.signature = signature;
    }

    @Override
    public void call( List<Object> args, Visitor<List<Object>,ProcedureException> visitor ) throws ProcedureException
    {
        try
        {
            Context.setGlobal( NashornUtil.unwrap( (ScriptObjectMirror) ctx.getAttribute( NashornScriptEngine.NASHORN_GLOBAL ) ) );
            ScriptRuntime.apply( procedureFunc, procedureFunc, procedureArguments( args, visitor ) );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Statement.ProcedureError, e, "Failed to invoke `%s`: %s", signature, e.getMessage() );
        }
    }

    private Object[] procedureArguments( List<Object> args, final Visitor<List<Object>,ProcedureException> visitor )
    {
        final Object[] argsPlusCtx = new Object[args.size() + 1];
        argsPlusCtx[0] = new EmitHandler( createOutputRecord(), visitor );
        for ( int i = 1; i < argsPlusCtx.length; i++ )
        {
            argsPlusCtx[i] = args.get(i-1);
        }
        return argsPlusCtx;
    }

    public class EmitHandler
    {
        private final List<Object> outputRecord;
        private final Visitor<List<Object>,ProcedureException> visitor;

        public EmitHandler( List<Object> outputRecord, Visitor<List<Object>,ProcedureException> visitor )
        {
            this.outputRecord = outputRecord;
            this.visitor = visitor;
        }

        public void apply( Object record ) throws ProcedureException
        {
            if( record instanceof ScriptObject )
            {
                mapper.translateRecord( (ScriptObject) record, outputRecord );
                visitor.visit( outputRecord );
            }
            else
            {
                throw new IllegalArgumentException( "Unable to emit " + record + ", unknown type `" + record.getClass() + "`." );
            }
        }
    }

    private ArrayList<Object> createOutputRecord()
    {
        // Output uses 'set' to set the output values after translating from javascript, so we need
        // an array list with the same number of empty slots as the output size.
        ArrayList<Object> objects = new ArrayList<>( signature.outputSignature().size() );
        for ( int i = 0; i < signature.outputSignature().size(); i++ )
        {
            objects.add( null );
        }
        return objects;
    }
}
