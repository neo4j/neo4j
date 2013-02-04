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

import static java.util.Collections.emptyList;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Schema;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;

public class SchemaImpl implements Schema
{

    private final ThreadToStatementContextBridge ctxProvider;

    public SchemaImpl(ThreadToStatementContextBridge ctxProvider)
    {
        this.ctxProvider = ctxProvider;
    }

    @Override
    public void createIndex( Label label, String propertyKey )
    {
        try
        {
            StatementContext ctx = ctxProvider.getCtxForWriting();
            ctx.addIndexRule( ctx.getOrCreateLabelId( label.name() ), propertyKey );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new ConstraintViolationException( "Unable to create index.", e );
        }
    }

    @Override
    public Iterable<String> getIndexes( Label label )
    {
        try
        {
            StatementContext ctx = ctxProvider.getCtxForReading();
            return ctx.getIndexRules( ctx.getLabelId( label.name() ));
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
    }
}
