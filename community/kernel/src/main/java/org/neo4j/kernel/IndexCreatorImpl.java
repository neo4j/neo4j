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

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyHolder;
import org.neo4j.kernel.impl.core.PropertyIndex;

public class IndexCreatorImpl implements IndexCreator
{
    private final Collection<String> propertyKeys;
    private final Label label;
    private final KeyHolder<PropertyIndex> propertyKeyManager;
    private final ThreadToStatementContextBridge ctxProvider;

    IndexCreatorImpl( ThreadToStatementContextBridge ctxProvider, KeyHolder<PropertyIndex> propertyKeyManager, Label label )
    {
        this.ctxProvider = ctxProvider;
        this.propertyKeyManager = propertyKeyManager;
        this.label = label;
        this.propertyKeys = new ArrayList<String>();
    }
    
    private IndexCreatorImpl( ThreadToStatementContextBridge ctxProvider,
            KeyHolder<PropertyIndex> propertyKeyManager, Label label, Collection<String> propertyKeys )
    {
        this.ctxProvider = ctxProvider;
        this.propertyKeyManager = propertyKeyManager;
        this.label = label;
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public IndexCreator on( String propertyKey )
    {
        if ( !propertyKeys.isEmpty() )
            throw new UnsupportedOperationException( "Compound indexes are not yet supported, only one property per index is allowed." );
        return new IndexCreatorImpl( ctxProvider, propertyKeyManager, label,
                addToCollection( asList( propertyKey ), new ArrayList<String>( propertyKeys ) ) );
    }

    @Override
    public IndexDefinition create() throws ConstraintViolationException
    {
        if ( propertyKeys.isEmpty() )
            throw new ConstraintViolationException( "An index needs at least one property key to index" );
        
        StatementContext context = ctxProvider.getCtxForWriting();
        try
        {
            String singlePropertyKey = single( propertyKeys );
            context.addIndexRule( context.getOrCreateLabelId( label.name() ),
                    propertyKeyManager.getOrCreateId( singlePropertyKey ) );
            return new IndexDefinitionImpl( ctxProvider, label, singlePropertyKey );
        }
        catch ( ConstraintViolationKernelException e )
        {
            throw new ConstraintViolationException( String.format(
                    "Unable to create index for label '%s' on properties %s.", label.name(), propertyKeys ), e );
        }
        finally
        {
            context.close();
        }
    }
}
