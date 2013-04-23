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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.neo4j.kernel.api.StatementContext;

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
        for ( Method method : StatementContext.class.getMethods() )
        {
            methods.add( new Object[]{method} );
        }
        return methods;
    }

    private final Method method;

    public CompositeStatementContextTest( Method method )
    {
        this.method = method;
    }

    @Test
    public void shouldDelegateInvocationAndInvokeBeforeAndAfterMethods() throws Exception
    {
        // given
        StatementContext delegate = mock( StatementContext.class );
        Object[] arguments = arguments( method );
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
        // Verify before and after methods - note: this does not know which methods are supposed to be read vs write
        if ( !"close".equals( method.getName() ) )
        {
            // Used to assert that we invoke the same operation
            ArgumentCaptor<ReadOrWrite> readORwrite = ArgumentCaptor.forClass( ReadOrWrite.class );

            InOrder order = inOrder( context.checking );
            order.verify( context.checking ).beforeOperation();
            order.verify( context.checking ).beforeReadOrWriteOperation( readORwrite.capture() );
            order.verify( context.checking ).afterReadOrWriteOperation( readORwrite.getValue() );
            order.verify( context.checking ).afterOperation();
            verifyNoMoreInteractions( context.checking );
        }
    }

    private final Random random = new Random();

    private Object[] arguments( Method method )
    {
        Class<?>[] parameters = method.getParameterTypes();
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
