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
package org.neo4j.kernel.extension;

import org.neo4j.annotations.service.Service;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.service.NamedService;

import static java.lang.String.format;

@Service
public abstract class ExtensionFactory<DEPENDENCIES> implements NamedService
{
    private final ExtensionType extensionType;
    private final String name;

    protected ExtensionFactory( String name )
    {
        this( ExtensionType.GLOBAL, name );
    }

    protected ExtensionFactory( ExtensionType extensionType, String name )
    {
        this.extensionType = extensionType;
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    /**
     * Create a new instance of this kernel extension.
     *
     * @param context the context the extension should be created for
     * @param dependencies deprecated
     * @return the {@link Lifecycle} for the extension
     */
    public abstract Lifecycle newInstance( ExtensionContext context, DEPENDENCIES dependencies );

    @Override
    public String toString()
    {
        return format( "Extension:%s[%s]", getClass().getSimpleName(), name );
    }

    ExtensionType getExtensionType()
    {
        return extensionType;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        final ExtensionFactory<?> that = (ExtensionFactory<?>) o;
        return name.equals( that.name );
    }
}
