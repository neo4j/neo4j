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
package org.neo4j.server.rest.repr;

import java.util.Iterator;

import org.neo4j.helpers.collection.ExceptionHandlingIterable;

public class RepresentationExceptionHandlingIterable<T> extends ExceptionHandlingIterable<T> {
    public RepresentationExceptionHandlingIterable(Iterable<T> source) {
        super(source);
    }

    @Override
    protected Iterator<T> exceptionOnIterator(Throwable t) {
        if (t instanceof Exception) rethrow(new BadInputException(t));
        return super.exceptionOnIterator(t);
    }

    @Override
    protected T exceptionOnNext(Throwable t) {
        if (t instanceof Exception) rethrow(new BadInputException(t));
        return super.exceptionOnNext(t);
    }

    @Override
    protected void exceptionOnRemove(Throwable t) {
        super.exceptionOnRemove(t);
    }

    @Override
    protected boolean exceptionOnHasNext(Throwable t) {
        if (t instanceof Exception) rethrow(new BadInputException(t));
        return super.exceptionOnHasNext(t);
    }
}
