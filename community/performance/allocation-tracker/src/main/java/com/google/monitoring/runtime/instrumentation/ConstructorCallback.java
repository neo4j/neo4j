/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package com.google.monitoring.runtime.instrumentation;

/**
 * This interface describes a function that is used to sample a
 * constructor.  It is intended to be invoked every time a constructor
 * for class T is invoked.  This will not be invoked when subclasses of
 * T are instantiated.
 * <p/>
 * This mechanism works independently of whether the class is part of the
 * JDK core library.
 *
 * @param <T> The class that will be sampled with this ConstructorCallback
 * @author Jeremy Manson
 */
public interface ConstructorCallback<T>
{
    /**
     * When an object implementing interface
     * <code>ConstructorCallback</code> is passed to {@link
     * com.google.monitoring.runtime.allocation.AllocationInspector#
     * addConstructorCallback(Class, ConstructorCallback)}, it will get executed
     * whenever a constructor for type T is invoked.
     *
     * @param newObj the new <code>Object</code> whose construction
     *               we're recording.  The object is not fully constructed; any
     *               references to this object that are stored in this callback are
     *               subject to the memory model constraints related to such
     *               objects.
     */
    public void sample( T newObj );
}
