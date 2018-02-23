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
package org.neo4j.server.scripting.javascript;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeJavaMethod;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mozilla.javascript.Context.enter;
import static org.mozilla.javascript.Context.exit;
import static org.mozilla.javascript.UniqueTag.NOT_FOUND;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.server.scripting.UserScriptClassWhiteList.getWhiteList;

class TestWhiteListJavaWrapper
{

    @AfterEach
    void exitContext()
    {
        try
        {
            exit();
        }
        catch ( IllegalStateException e )
        {
            // Om nom nom
        }
    }

    @Test
    void shouldBlockAttemptsAtAccessingClassLoader()
    {
        assertThrows( SecurityException.class, () -> {
            // Given
            WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( new HashSet<>() ) );

            // When
            wrapper.wrap( null, null, getClass().getClassLoader(), null );
        } );
    }

    @Test
    void shouldDownCastSubclassesToAllowedParentClass()
    {
        // Given
        Set<String> whiteList = new HashSet<>();
        whiteList.add( Object.class.getName() );

        WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( whiteList ) );

        Context cx = enter();
        Scriptable scope = cx.initStandardObjects();

        // When
        Object wrapped = wrapper.wrap( cx, scope, new TestWhiteListJavaWrapper(), null );

        // Then
        assertThat( wrapped, is( instanceOf( NativeJavaObject.class ) ) );
        NativeJavaObject obj = (NativeJavaObject)wrapped;

        assertThat( obj.has( "aGetter", scope ), is( false ));
        assertThat( obj.get( "aGetter", scope ), is( NOT_FOUND ) );
    }

    @Test
    void shouldThrowSecurityExceptionWhenAccessingLockedClasses()
    {
        assertThrows( SecurityException.class, () -> {
            // Given
            Set<String> whiteList = new HashSet<>();
            whiteList.add( Object.class.getName() );

            WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( whiteList ) );

            Context cx = enter();
            Scriptable scope = cx.initStandardObjects();

            // When
            Object wrapped = wrapper.wrap( cx, scope, TestWhiteListJavaWrapper.class, null );
        } );
    }

    @Test
    void shouldAllowAccessToWhiteListedClassMembers()
    {
        // XXX: The Rhino security stuff can only be set globally, unfortunately. That means
        // that we need to use a class here that is white-listed in the "real" white list, because
        // other tests will already have configured global security settings that we cannot override.

        // Given
        WhiteListJavaWrapper wrapper =
                new WhiteListJavaWrapper( new WhiteListClassShutter( getWhiteList() ) );

        Context cx = enter();
        Scriptable scope = cx.initStandardObjects();

        // When
        Object wrapped = wrapper.wrap( cx, scope, withName( "blah" ), null );

        // Then
        assertThat( wrapped, is( instanceOf( NativeJavaObject.class ) ) );
        NativeJavaObject obj = (NativeJavaObject)wrapped;

        assertThat( obj.get( "name", scope ),
                is( instanceOf( NativeJavaMethod.class ) ) );
    }
}
