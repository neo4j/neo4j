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
package org.neo4j.codegen;

import java.util.function.Consumer;

public interface MethodEmitter
{
    void done();

    void expression( Expression expression );

    void put( Expression target, FieldReference field, Expression value );

    void returns();

    void returns( Expression value );

    void assign( LocalVariable local, Expression value );

    void beginWhile( Expression test );

    void beginIf( Expression test );

    void beginBlock();

    void endBlock();

    <T> void tryCatchBlock( Consumer<T> body, Consumer<T> handler, LocalVariable exception, T block );

    void throwException( Expression exception );

    void declare( LocalVariable local );

    void assignVariableInScope( LocalVariable local, Expression value );
}
