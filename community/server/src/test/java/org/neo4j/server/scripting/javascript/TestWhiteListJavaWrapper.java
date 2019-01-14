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
package org.neo4j.server.scripting.javascript;

import org.junit.After;
import org.junit.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeJavaMethod;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.UniqueTag;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.scripting.UserScriptClassWhiteList;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestWhiteListJavaWrapper
{

    @After
    public void exitContext()
    {
        try
        {
            Context.exit();
        }
        catch ( IllegalStateException e )
        {
            // Om nom nom
        }
    }

    @Test( expected = SecurityException.class )
    public void shouldBlockAttemptsAtAccessingClassLoader()
    {
        // Given
        WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( new HashSet<>() ) );

        // When
        wrapper.wrap( null, null, getClass().getClassLoader(), null );
    }

    @Test
    public void shouldDownCastSubclassesToAllowedParentClass()
    {
        // Given
        Set<String> whiteList = new HashSet<>();
        whiteList.add( Object.class.getName() );

        WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( whiteList ) );

        Context cx = Context.enter();
        Scriptable scope = cx.initStandardObjects();

        // When
        Object wrapped = wrapper.wrap( cx, scope, new TestWhiteListJavaWrapper(), null );

        // Then
        assertThat( wrapped, is( instanceOf( NativeJavaObject.class ) ) );
        NativeJavaObject obj = (NativeJavaObject)wrapped;

        assertThat( obj.has( "aGetter", scope ), is( false ));
        assertThat( obj.get( "aGetter", scope ), is( UniqueTag.NOT_FOUND ) );
    }

    @Test( expected = SecurityException.class )
    public void shouldThrowSecurityExceptionWhenAccessingLockedClasses()
    {
        // Given
        Set<String> whiteList = new HashSet<>();
        whiteList.add( Object.class.getName() );

        WhiteListJavaWrapper wrapper = new WhiteListJavaWrapper( new WhiteListClassShutter( whiteList ) );

        Context cx = Context.enter();
        Scriptable scope = cx.initStandardObjects();

        // When
        Object wrapped = wrapper.wrap( cx, scope, TestWhiteListJavaWrapper.class, null );
    }

    @Test
    public void shouldAllowAccessToWhiteListedClassMembers()
    {
        // XXX: The Rhino security stuff can only be set globally, unfortunately. That means
        // that we need to use a class here that is white-listed in the "real" white list, because
        // other tests will already have configured global security settings that we cannot override.

        // Given
        WhiteListJavaWrapper wrapper =
                new WhiteListJavaWrapper( new WhiteListClassShutter( UserScriptClassWhiteList.getWhiteList() ) );

        Context cx = Context.enter();
        Scriptable scope = cx.initStandardObjects();

        // When
        Object wrapped = wrapper.wrap( cx, scope, RelationshipType.withName( "blah" ), null );

        // Then
        assertThat( wrapped, is( instanceOf( NativeJavaObject.class ) ) );
        NativeJavaObject obj = (NativeJavaObject)wrapped;

        assertThat( obj.get( "name", scope ),
                is( instanceOf( NativeJavaMethod.class ) ) );
    }
}
