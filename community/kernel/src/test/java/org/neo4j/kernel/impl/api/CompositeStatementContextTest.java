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
package org.neo4j.kernel.impl.api;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InOrder;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.operations.ReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.WriteOperations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(Parameterized.class)
public class CompositeStatementContextTest
{
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> allMethods()
    {
        List<Object[]> methods = new ArrayList<Object[]>();
        for ( Method method : ReadOperations.class.getMethods() )
        {
            methods.add( new Object[]{new OperationMethod( ReadOrWrite.READ, method )} );
        }
        for ( Method method : WriteOperations.class.getMethods() )
        {
            methods.add( new Object[]{new OperationMethod( ReadOrWrite.WRITE, method )} );
        }
        for ( Method method : SchemaStateOperations.class.getMethods() )
        {
            methods.add( new Object[]{new OperationMethod( null, method )} );
        }
        return methods;
    }

    private final OperationMethod method;

    public CompositeStatementContextTest( OperationMethod method )
    {
        this.method = method;
    }

    @Test
    public void shouldDelegateInvocationAndInvokeBeforeAndAfterMethods() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        Object[] arguments = arguments( method.getParameterTypes() );
        Object expectedResult = null;
        if ( method.getReturnType() != void.class )
        {
            expectedResult = sample( method.getReturnType() );
            method.invoke( doReturn( expectedResult ).when( delegate ), arguments );
        }

        MyStatementContext context = new MyStatementContext( delegate );

        // when
        Object result = method.invoke( context, arguments );

        // then
        method.invoke( verify( delegate ), arguments );
        verifyNoMoreInteractions( delegate );
        if ( method.getReturnType() != void.class )
        {
            assertEquals( expectedResult, result );
        }
        // Verify before and after methods

        InOrder order = inOrder( context.checking );
        order.verify( context.checking ).beforeOperation();
        if ( method.operation != null )
        {
            order.verify( context.checking ).beforeReadOrWriteOperation( method.operation );
            order.verify( context.checking ).afterReadOrWriteOperation( method.operation );
        }
        order.verify( context.checking ).afterOperation();
        verifyNoMoreInteractions( context.checking );
    }

    private static class OperationMethod
    {
        final ReadOrWrite operation;
        private final Method method;

        OperationMethod( ReadOrWrite operation, Method method )
        {
            this.operation = operation;
            this.method = method;
        }

        public Class<?> getReturnType()
        {
            return method.getReturnType();
        }

        @Override
        public String toString()
        {
            return String.format( "%s#%s(...)", method.getDeclaringClass().getName(), method.getName() );
        }

        public Object invoke( Object obj, Object... args )
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
        {
            return method.invoke( obj, args );
        }

        public Class<?>[] getParameterTypes()
        {
            return method.getParameterTypes();
        }
    }

    private final Random random = new Random();

    private Object[] arguments( Class<?>[] parameters )
    {
        Object[] arguments = new Object[parameters.length];
        for ( int i = 0; i < arguments.length; i++ )
        {
            arguments[i] = sample( parameters[i] );
        }
        return arguments;
    }

    private Object sample( Class<?> type )
    {
        if ( type.isPrimitive() )
        {
            if ( type == long.class )
            {
                return random.nextLong();
            }
            if ( type == boolean.class )
            {
                return random.nextBoolean();
            }
        }
        else if ( type.isEnum() )
        {
            Object[] candidates = type.getEnumConstants();
            return candidates[random.nextInt( candidates.length )];
        }
        else if ( !type.isArray() )
        {
            if ( type == String.class )
            {
                StringBuilder result = new StringBuilder();
                for ( int len = 5 + random.nextInt( 10 ); len > 0; len-- )
                {
                    result.append( 'a' + random.nextInt( 'z' - 'a' ) );
                }
                return result.toString();
            }
            if ( type == Long.class )
            {
                return random.nextLong();
            }
            return mock( type );
        }
        throw new UnsupportedOperationException( "doesn't support " + type + " please add support for it." );
    }

    private enum ReadOrWrite
    {
        READ, WRITE
    }

    private interface Checking
    {
        void beforeOperation();

        void afterOperation();

        void beforeReadOrWriteOperation( ReadOrWrite row );

        void afterReadOrWriteOperation( ReadOrWrite row );
    }

    private class MyStatementContext extends CompositeStatementContext
    {
        final Checking checking;

        MyStatementContext( StatementContext delegate )
        {
            super( delegate );
            this.checking = mock( Checking.class );
        }

        @Override
        protected void beforeOperation()
        {
            checking.beforeOperation();
        }

        @Override
        protected void afterOperation()
        {
            checking.afterOperation();
        }

        @Override
        protected void beforeReadOperation()
        {
            checking.beforeReadOrWriteOperation( ReadOrWrite.READ );
        }

        @Override
        protected void afterReadOperation()
        {
            checking.afterReadOrWriteOperation( ReadOrWrite.READ );
        }

        @Override
        protected void beforeWriteOperation()
        {
            checking.beforeReadOrWriteOperation( ReadOrWrite.WRITE );
        }

        @Override
        protected void afterWriteOperation()
        {
            checking.afterReadOrWriteOperation( ReadOrWrite.WRITE );
        }
    }
}
