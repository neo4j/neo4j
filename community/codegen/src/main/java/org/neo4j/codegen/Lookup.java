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

abstract class Lookup<T>
{
    static Lookup<FieldReference> field( final TypeReference type, final String name )
    {
        return new Lookup<FieldReference>()
        {
            @Override
            FieldReference lookup( CodeBlock method )
            {
                FieldReference field = method.clazz.getField( name );
                if ( field == null )
                {
                    throw new IllegalArgumentException(
                            method.clazz.handle() + " has no such field: " + name + " of type " + type );
                }
                else if ( !type.equals( field.type() ) )
                {
                    throw new IllegalArgumentException(
                            method.clazz.handle() + " has no such field: " + name + " of type " + type +
                            ", actual field has type: " + field.type() );
                }
                return field;
            }
        };
    }

    abstract T lookup( CodeBlock method );
}
