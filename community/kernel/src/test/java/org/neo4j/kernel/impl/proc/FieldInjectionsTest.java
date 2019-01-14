/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.Context;

import static org.junit.Assert.assertEquals;

public class FieldInjectionsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotAllowClassesWithNonInjectedFields() throws Throwable
    {
        // Given
        FieldInjections injections = new FieldInjections( new ComponentRegistry() );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Field `someState` on `ProcedureWithNonInjectedMemberFields` " +
                                 "is not annotated as a @Context and is not static. " +
                                 "If you want to store state along with your procedure, " +
                                 "please use a static field." );

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
        components.register( int.class, ctx -> 1337 );
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

    @Test
    public void syntheticsAllowed() throws Throwable
    {
        // Given
        ComponentRegistry components = new ComponentRegistry();
        components.register( int.class, ctx -> 1337 );
        FieldInjections injections = new FieldInjections( components );

        // When
        List<FieldInjections.FieldSetter> setters = injections.setters( Outer.ClassWithSyntheticField.class );

        // Then
        Outer.ClassWithSyntheticField syntheticField = new Outer().classWithSyntheticField();
        for ( FieldInjections.FieldSetter setter : setters )
        {
            setter.apply( null, syntheticField );
        }

        assertEquals( 1337, syntheticField.innerField );
    }

    public static class ProcedureWithNonInjectedMemberFields
    {
        public boolean someState;
    }

    public static class ProcedureWithPrivateMemberField
    {
        @Context
        private boolean someState;
    }

    public static class ProcedureWithStaticFields
    {
        private static boolean someState;
    }

    public static class ParentProcedure
    {
        @Context
        public int parentField;
    }

    public static class ChildProcedure extends ParentProcedure
    {
        @Context
        public int childField;
    }

    //The outer class is just here to force a synthetic field in the inner class.
    //This is not a realistic scenario but we merely want to make sure the loader
    //does not choke on synthetic fields since compilers, e.g. groovy, can generate
    //these.
    public static class Outer
    {
        ClassWithSyntheticField classWithSyntheticField()
        {
            return new ClassWithSyntheticField();
        }

        public class ClassWithSyntheticField
        {
            //this class will have a generated field:
            //synthetic Outer this$0;

            @Context
            public int innerField;
        }
    }
}
