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

package org.neo4j.graphdb.config;

import java.util.Optional;

/**
 * All fields specified here are set via annotations when loaded
 */
public abstract class BaseSetting<T> implements Setting<T>
{
    private boolean deprecated = false;
    private String replacement = null;
    private boolean internal = false;
    private String documentedDefaultValue = null;
    private String description = null;

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
}
