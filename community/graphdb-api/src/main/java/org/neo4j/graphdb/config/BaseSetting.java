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
package org.neo4j.graphdb.config;

import java.util.Optional;

/**
 * All fields specified here are set via annotations when loaded
 * @deprecated The settings API will be completely rewritten in 4.0
 */
@Deprecated
public abstract class BaseSetting<T> implements Setting<T>
{
    private boolean deprecated;
    private String replacement;
    private boolean internal;
    private boolean secret;
    private boolean dynamic;
    private String documentedDefaultValue;
    private String description;

    @Override
    public boolean deprecated()
    {
        return this.deprecated;
    }

    public void setDeprecated( final boolean val )
    {
        this.deprecated = val;
    }

    @Override
    public Optional<String> replacement()
    {
        return Optional.ofNullable( this.replacement );
    }

    public void setReplacement( final String val )
    {
        this.replacement = val;
    }

    @Override
    public boolean internal()
    {
        return this.internal;
    }

    public void setInternal( final boolean val )
    {
        this.internal = val;
    }

    @Override
    public boolean secret()
    {
        return this.secret;
    }

    public void setSecret( final boolean val )
    {
        this.secret = val;
    }

    @Override
    public Optional<String> documentedDefaultValue()
    {
        return Optional.ofNullable( this.documentedDefaultValue );
    }

    public void setDocumentedDefaultValue( final String val )
    {
        this.documentedDefaultValue = val;
    }

    @Override
    public Optional<String> description()
    {
        return Optional.ofNullable( description );
    }

    public void setDescription( final String description )
    {
        this.description = description;
    }

    @Override
    public String toString()
    {
        return valueDescription();
    }

    /**
     * Checks whether this setting is dynamic or not. Dynamic properties are allowed to be changed at runtime without
     * restarting the server.
     *
     * @return {@code true} if this setting can be changed at runtime.
     */
    @Override
    public boolean dynamic()
    {
        return dynamic;
    }

    public void setDynamic( boolean dynamic )
    {
        this.dynamic = dynamic;
    }
}
