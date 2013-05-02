/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;

class AbstractConstraintCreator implements ConstraintCreator
{
    protected final ThreadToStatementContextBridge ctxProvider;
    protected final DependencyResolver dependencyResolver;
    protected final Label label;

    AbstractConstraintCreator( ThreadToStatementContextBridge ctxProvider,
            DependencyResolver dependencyResolver, Label label )
    {
        this.ctxProvider = ctxProvider;
        this.dependencyResolver = dependencyResolver;
        this.label = label;
    }

    @Override
    public ConstraintCreator on( String propertyKey )
    {
        return new PropertyConstraintCreator( ctxProvider, dependencyResolver, label, propertyKey );
    }

    @Override
    public ConstraintCreator unique()
    {
        return new PropertyUniqueConstraintCreator( ctxProvider, dependencyResolver, label, null );
    }

    @Override
    public ConstraintDefinition create()
    {
        throw new IllegalStateException( "Not constraint assertions specified" );
    }
}
