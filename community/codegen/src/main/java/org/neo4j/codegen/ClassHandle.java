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

public class ClassHandle extends TypeReference
{
    private final TypeReference parent;
    final CodeGenerator generator;
    private final long generation;

    ClassHandle( String packageName, String name, TypeReference parent, CodeGenerator generator, long generation )
    {
        super(packageName, name);
        this.parent = parent;
        this.generator = generator;
        this.generation = generation;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj == this;
    }

    @Override
    public int hashCode()
    {
        return simpleName().hashCode();
    }

    public Object newInstance() throws CompilationFailureException, IllegalAccessException, InstantiationException
    {
        return loadClass().newInstance();
    }

    public Class<?> loadClass() throws CompilationFailureException
    {
        return generator.loadClass( name(), generation );
    }

    public TypeReference parent()
    {
        return parent;
    }
}
