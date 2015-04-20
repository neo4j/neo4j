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
package org.neo4j.test.subprocess;

import com.sun.jdi.AbsentInformationException;
import com.sun.jdi.ClassNotLoadedException;
import com.sun.jdi.IncompatibleThreadStateException;
import com.sun.jdi.InvalidTypeException;
import com.sun.jdi.LocalVariable;
import com.sun.jdi.StackFrame;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;
import java.io.PrintStream;

@SuppressWarnings( "restriction" )
public class DebugInterface
{
    private final com.sun.jdi.event.LocatableEvent event;
    private final SubProcess.DebugDispatch debug;

    DebugInterface( SubProcess.DebugDispatch debug, com.sun.jdi.event.LocatableEvent event )
    {
        this.debug = debug;
        this.event = event;
    }

    public boolean matchCallingMethod( int offset, Class<?> owner, String method )
    {
        try
        {
            com.sun.jdi.Location location = event.thread().frame( offset ).location();
            if ( owner != null )
            {
                if ( !owner.getName().equals( location.declaringType().name() ) ) return false;
            }
            if ( method != null )
            {
                if ( !method.equals( location.method().name() ) ) return false;
            }
            return true;
        }
        catch ( com.sun.jdi.IncompatibleThreadStateException e )
        {
            return false;
        }
    }

    public DebuggedThread thread()
    {
        return new DebuggedThread( debug, event.thread() );
    }

    public void printStackTrace( PrintStream out )
    {
        thread().printStackTrace(out);
    }

    public Object getLocalVariable( String name )
    {
        try
        {
            StackFrame frame = event.thread().frame( 0 );
            return fromMirror( frame.getValue( frame.visibleVariableByName( name ) ) );
        }
        catch ( IncompatibleThreadStateException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( AbsentInformationException e )
        {
            throw new IllegalStateException( e );
        }
    }

    public void setLocalVariable( String name, Object value )
    {
        try
        {
            StackFrame frame = event.thread().frame( 0 );
            LocalVariable local = frame.visibleVariableByName( name );
            frame.setValue( local, mirror( value ) );
        }
        catch ( IncompatibleThreadStateException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( AbsentInformationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( InvalidTypeException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( ClassNotLoadedException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @SuppressWarnings( "boxing" )
    private Value mirror( Object value )
    {
        VirtualMachine vm = event.virtualMachine();
        if ( value == null )
        {
            return vm.mirrorOfVoid();
        }
        if ( value instanceof String )
        {
            return vm.mirrorOf( (String) value );
        }
        if ( value instanceof Integer )
        {
            return vm.mirrorOf( (Integer) value );
        }
        if ( value instanceof Long )
        {
            return vm.mirrorOf( (Long) value );
        }
        if ( value instanceof Double )
        {
            return vm.mirrorOf( (Double) value );
        }
        if ( value instanceof Boolean )
        {
            return vm.mirrorOf( (Boolean) value );
        }
        if ( value instanceof Byte )
        {
            return vm.mirrorOf( (Byte) value );
        }
        if ( value instanceof Character )
        {
            return vm.mirrorOf( (Character) value );
        }
        if ( value instanceof Short )
        {
            return vm.mirrorOf( (Short) value );
        }
        if ( value instanceof Float )
        {
            return vm.mirrorOf( (Float) value );
        }
        throw new IllegalArgumentException( "Cannot mirror: " + value );
    }

    @SuppressWarnings( "boxing" )
    private Object fromMirror( Value value )
    {
        if ( value instanceof com.sun.jdi.VoidValue )
        {
            return null;
        }
        if ( value instanceof com.sun.jdi.StringReference )
        {
            return ( (com.sun.jdi.StringReference) value).value();
        }
        if ( value instanceof com.sun.jdi.IntegerValue )
        {
            return ( (com.sun.jdi.IntegerValue) value ).intValue();
        }
        if ( value instanceof com.sun.jdi.LongValue )
        {
            return ( (com.sun.jdi.LongValue) value ).longValue();
        }
        if ( value instanceof com.sun.jdi.DoubleValue )
        {
            return ( (com.sun.jdi.DoubleValue) value ).doubleValue();
        }
        if ( value instanceof com.sun.jdi.BooleanValue )
        {
            return ( (com.sun.jdi.BooleanValue) value ).booleanValue();
        }
        if ( value instanceof com.sun.jdi.ByteValue )
        {
            return ( (com.sun.jdi.ByteValue) value ).byteValue();
        }
        if ( value instanceof com.sun.jdi.CharValue )
        {
            return ( (com.sun.jdi.CharValue) value ).charValue();
        }
        if ( value instanceof com.sun.jdi.ShortValue )
        {
            return ( (com.sun.jdi.ShortValue) value ).shortValue();
        }
        if ( value instanceof com.sun.jdi.FloatValue )
        {
            return ( (com.sun.jdi.FloatValue) value ).floatValue();
        }
        return value.toString();
    }
}