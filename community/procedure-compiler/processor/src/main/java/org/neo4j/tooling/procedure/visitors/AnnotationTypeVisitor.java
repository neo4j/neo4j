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
package org.neo4j.tooling.procedure.visitors;

import java.lang.annotation.Annotation;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.SimpleElementVisitor8;

public class AnnotationTypeVisitor extends SimpleElementVisitor8<Boolean,Void>
{

    private final Class<? extends Annotation> annotationType;

    public AnnotationTypeVisitor( Class<? extends Annotation> annotationType )
    {
        this.annotationType = annotationType;
    }

    @Override
    public Boolean visitType( TypeElement element, Void aVoid )
    {
        return element.getQualifiedName().contentEquals( annotationType.getName() );
    }
}
