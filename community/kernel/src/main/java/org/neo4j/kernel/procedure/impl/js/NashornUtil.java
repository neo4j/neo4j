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

import jdk.nashorn.api.scripting.AbstractJSObject;
import jdk.nashorn.api.scripting.JSObject;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.ScriptObject;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

public class NashornUtil
{
    private static final MethodHandle getScriptObject = initGetScriptObject();

    /**
     * This is a bit awful - but the Nashorn API exposes heavily wrapped objects to us. Every single
     * field access (such as reading a single integer) from
     * java means accessing a thread-local, creating a callable object and boxing the integer. For our use
     * case (streaming hundreds of thousands of records
     * per second for months or years on end) this is completely unacceptable.
     *
     * However, Nashorn's internal representation of JavaScript objects has extensive support for reading primitives.
     * This function unwraps the protective layer (which isn't good) and allows us efficient access to javascript objects
     * (which is good). The safety of this is hinged on test coverage for procedure output.
     *
     * JMH measurements show that this gives a baseline of 15% higher throughput. However, it then enables raw access
     * to unboxed primitives (getInteger) etc, using those improves performance by one order of magnitude
     * (yield 7M records/s per thread vs 500K/s per thread). Obviously we can't leverage that until
     * Cypher internals allow us to move unboxed primitives around, but that day will come!
     */
    public static ScriptObject unwrap(ScriptObjectMirror mirror) throws Throwable
    {
        return (ScriptObject) getScriptObject.invokeExact( mirror );
    }

    private static MethodHandle initGetScriptObject()
    {
        try
        {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Method method = ScriptObjectMirror.class.getDeclaredMethod( "getScriptObject" );
            method.setAccessible( true );
            return lookup.unreflect( method );
        }
        catch( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    /** Expose a method handle in a way nashorn understands */
    public static JSObject asJSFunction( final MethodHandle method )
    {
        return new AbstractJSObject()
        {
            @Override
            public boolean isFunction()
            {
                return true;
            }

            @Override
            public Object call( Object thiz, Object... args )
            {
                try
                {
                    return method.invokeWithArguments( args );
                }
                catch ( Throwable throwable )
                {
                    throw new RuntimeException( throwable );
                }
            }
        };
    }
}
