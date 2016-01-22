/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.kernel.api.exceptions.ProcedureException;

import static junit.framework.TestCase.assertEquals;

public class FieldInjectionsTest
{
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotAllowClassesWithNonInjectedFields() throws Throwable
    {
        // Given
        FieldInjections injections = new FieldInjections( new ComponentRegistry() );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Field `someState` on `ProcedureWithNonInjectedMemberFields` is not annotated as a @Resource and is not static. " +
                                 "If you want to store state along with your procedure, please use a static field." );

        // When
        injections.setters( ProcedureWithNonInjectedMemberFields.class );
    }

    @Test
    public void shouldNotAllowNonPublicFieldsForInjection() throws Throwable
    {
        // Given
        FieldInjections injections = new FieldInjections( new ComponentRegistry() );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Field `someState` on `ProcedureWithPrivateMemberField` must be non-final and public." );

        // When
        injections.setters( ProcedureWithPrivateMemberField.class );
    }

    @Test
    public void staticFieldsAreAllowed() throws Throwable
    {
        // Given
        FieldInjections injections = new FieldInjections( new ComponentRegistry() );

        // When
        List<FieldInjections.FieldSetter> setters = injections.setters( ProcedureWithStaticFields.class );

        // Then
        assertEquals( 0, setters.size() );
    }

    @Test
    public void inheritanceIsAllowed() throws Throwable
    {
        // Given
        ComponentRegistry components = new ComponentRegistry();
        components.register( int.class, (ctx) -> 1337 );
        FieldInjections injections = new FieldInjections( components );

        // When
        List<FieldInjections.FieldSetter> setters = injections.setters( ChildProcedure.class );

        // Then
        ChildProcedure childProcedure = new ChildProcedure();
        for ( FieldInjections.FieldSetter setter : setters )
        {
            setter.apply( null, childProcedure );
        }

        assertEquals( 1337, childProcedure.childField );
        assertEquals( 1337, childProcedure.parentField );
    }

    public static class ProcedureWithNonInjectedMemberFields
    {
        public boolean someState = false;
    }

    public static class ProcedureWithPrivateMemberField
    {
        @Resource
        private boolean someState = false;
    }

    public static class ProcedureWithStaticFields
    {
        private static boolean someState = false;
    }

    public static class ParentProcedure
    {
        @Resource
        public int parentField;
    }

    public static class ChildProcedure extends ParentProcedure
    {
        @Resource
        public int childField;
    }
}