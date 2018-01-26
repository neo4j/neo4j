/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.procedure.validation.model;

import java.util.List;

public class UserFunctionDefinition
{
    private final ExtensionConstructor constructor;
    private final ExtensionQualifiedName name;
    private final List<ExtensionParameter> parameters;
    private final ElementType returnType;
    private final List<ExtensionField> extensionFields;

    public UserFunctionDefinition( ExtensionConstructor constructor, ExtensionQualifiedName name,
            List<ExtensionParameter> parameters, ElementType returnType, List<ExtensionField> extensionFields )
    {
        this.constructor = constructor;
        this.name = name;
        this.parameters = parameters;
        this.returnType = returnType;
        this.extensionFields = extensionFields;
    }

    public ExtensionConstructor getConstructor()
    {
        return constructor;
    }

    public ExtensionQualifiedName getName()
    {
        return name;
    }

    public List<ExtensionParameter> getParameters()
    {
        return parameters;
    }

    public ElementType getReturnType()
    {
        return returnType;
    }

    public List<ExtensionField> getExtensionFields()
    {
        return extensionFields;
    }
}
