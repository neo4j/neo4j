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

import java.util.function.Function;

/**
 * @deprecated The settings API will be completely rewritten in 4.0
 */
@Deprecated
public abstract class ScopeAwareSetting<T> extends BaseSetting<T>
{
    private Function<String,String> scopingRule = Function.identity();

    @Override
    public final String name()
    {
        return scopingRule.apply( provideName() );
    }

    protected abstract String provideName();

    @Override
    public void withScope( Function<String,String> scopingRule )
    {
        this.scopingRule = scopingRule;
    }
}
