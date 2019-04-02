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
package org.neo4j.test.extension.guard;

import org.objectweb.asm.AnnotationVisitor;

import java.util.Set;

public class AnnotationClassCollectorVisitor extends AnnotationVisitor
{
    private final Set<String> descriptors;

    AnnotationClassCollectorVisitor( int apiVersion, Set<String> descriptors )
    {
        super( apiVersion );
        this.descriptors = descriptors;
    }

    @Override
    public void visitEnum( String name, String descriptor, String value )
    {
        descriptors.add( descriptor );
    }
}
