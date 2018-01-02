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

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;

import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.WrapFactory;

public class WhiteListJavaWrapper extends WrapFactory
{

    private final ClassShutter classShutter;

    public WhiteListJavaWrapper( ClassShutter classShutter )
    {
        this.classShutter = classShutter;
    }

    public static class JavascriptJavaObject extends NativeJavaObject
    {
        public JavascriptJavaObject( Scriptable scope, Object javaObject, Class type )
        {
            // we pass 'null' to object. NativeJavaObject uses
            // passed 'type' to reflect fields and methods when
            // object is null.
            super(scope, null, type);

            // Now, we set actual object. 'javaObject' is protected
            // field of NativeJavaObject.
            this.javaObject = javaObject;
        }
    }

    /*
     * Majority of code below from Rhino source, written by A. Sundararajan.
     */
    @Override
    public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, Object javaObject, Class staticType) {

        if (javaObject instanceof ClassLoader) {

            throw new SecurityException( "Class loaders cannot be accessed in this environment." );

        } else
        {
            String name = null;
            if (javaObject instanceof Class) {
                name = ((Class)javaObject).getName();
            } else if (javaObject instanceof Member ) {
                Member member = (Member) javaObject;
                // Check member access. Don't allow reflective access to
                // non-public members. Note that we can't call checkMemberAccess
                // because that expects exact stack depth!
                if (!Modifier.isPublic( member.getModifiers() )) {
                    return null;
                }
                name = member.getDeclaringClass().getName();
            }

            // Now, make sure that no ClassShutter prevented Class or Member
            // of it is accessed reflectively. Note that ClassShutter may
            // prevent access to a class, even though SecurityManager permit.
            if (name != null) {
                if (!classShutter.visibleToScripts(name)) {
                    throw new SecurityException( "'"+name+"' cannot be accessed in this environment." );
                } else {
                    return new NativeJavaObject(scope, javaObject, staticType);
                }
            }
        }

        // we have got some non-reflective object.
        Class dynamicType = javaObject.getClass();
        String name = dynamicType.getName();
        if (!classShutter.visibleToScripts(name)) {
            // Object of some sensitive class (such as sun.net.www.*
            // objects returned from public method of java.net.URL class.
            // We expose this object as though it is an object of some
            // super class that is safe for access.

            Class type = null;

            // Whenever a Java Object is wrapped, we are passed with a
            // staticType which is the type found from environment. For
            // example, method return type known from signature. The dynamic
            // type would be the actual Class of the actual returned object.
            // If the staticType is an interface, we just use that type.
            if (staticType != null && staticType.isInterface()) {
                type = staticType;
            } else {
                // dynamicType is always a class type and never an interface.
                // find an accessible super class of the dynamic type.
                while (true) {
                    dynamicType = dynamicType.getSuperclass();

                    if(dynamicType != null)
                    {
                        name = dynamicType.getName();
                        if (classShutter.visibleToScripts(name)) {
                            type = dynamicType;
                            break;
                        }
                    } else
                    {
                        break;
                    }
                }
                // atleast java.lang.Object has to be accessible. So, when
                // we reach here, type variable should not be null.
                assert type != null:
                        "even java.lang.Object is not accessible?";
            }
            // create custom wrapper with the 'safe' type.
            return new JavascriptJavaObject(scope, javaObject, type);
        } else {
            return new NativeJavaObject(scope, javaObject, staticType);
        }
    }
}
