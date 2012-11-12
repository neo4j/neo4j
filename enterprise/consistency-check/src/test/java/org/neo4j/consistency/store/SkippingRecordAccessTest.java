/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.store;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.helpers.Exceptions;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(Suite.class)
@Suite.SuiteClasses({SkippingRecordAccessTest.TestRecordReferenceMethods.class,
                     SkippingRecordAccessTest.TestChangedRecordMethods.class})
public class SkippingRecordAccessTest
{
    @RunWith(Parameterized.class)
    @SuppressWarnings("unchecked")
    public static class TestRecordReferenceMethods
    {
        // given
        private final SkippingRecordAccess recordAccess = new SkippingRecordAccess();
        private final Method method;

        @Test
        public void shouldSkipAnyReferencedRecord() throws Exception
        {
            PendingReferenceCheck pending = mock( PendingReferenceCheck.class );

            // when
            try
            {
                ((RecordReference) invoke( recordAccess, method )).dispatch( pending );
            }
            catch ( Exception cause )
            {
                throw Exceptions.withCause( new AssertionError( methodString( method ) ), cause );
            }

            // then
            verify( pending ).skip();
            verifyNoMoreInteractions( pending );
        }

        public TestRecordReferenceMethods( Method method )
        {
            this.method = method;
        }

        @Parameterized.Parameters
        public static List<Object[]> methods()
        {
            return allMethods( true );
        }
    }

    @RunWith(Parameterized.class)
    @SuppressWarnings("unchecked")
    public static class TestChangedRecordMethods
    {
        // given
        private final SkippingRecordAccess recordAccess = new SkippingRecordAccess();
        private final Method method;

        @Test
        public void shouldReturnNullForChangedRecords() throws Exception
        {
            // when
            Object result;
            try
            {
                result = invoke( recordAccess, method );
            }
            catch ( Exception cause )
            {
                throw Exceptions.withCause( new AssertionError( methodString( method ) ), cause );
            }

            // then
            assertNull( result );
        }

        public TestChangedRecordMethods( Method method )
        {
            this.method = method;
        }

        @Parameterized.Parameters
        public static List<Object[]> methods()
        {
            return allMethods( false );
        }
    }

    private static String methodString( Method method )
    {
        StringBuilder result = new StringBuilder( method.getName() ).append( '(' );
        for ( Class<?> param : method.getParameterTypes() )
        {
            result.append( param.getName() );
        }
        return result.append( ')' ).toString();
    }

    private static Object invoke( SkippingRecordAccess obj, Method method ) throws Exception
    {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] parameters = new Object[parameterTypes.length];
        for ( int i = 0; i < parameters.length; i++ )
        {
            parameters[i] = (parameterTypes[i] == int.class) ? intArg() : longArg();
        }
        try
        {
            return method.invoke( obj, parameters );
        }
        catch ( InvocationTargetException e )
        {
            throw Exceptions.launderedException( Exception.class, e.getTargetException() );
        }
    }

    private static List<Object[]> allMethods( boolean returnsReference )
    {
        List<Object[]> result = new ArrayList<Object[]>();
        for ( Method method : DiffRecordAccess.class.getMethods() )
        {
            if ( (method.getReturnType() == RecordReference.class) == returnsReference )
            {
                result.add( new Object[]{method} );
            }
        }
        return result;
    }

    private static Object intArg()
    {
        return 0;
    }

    private static Object longArg()
    {
        return 0L;
    }
}
