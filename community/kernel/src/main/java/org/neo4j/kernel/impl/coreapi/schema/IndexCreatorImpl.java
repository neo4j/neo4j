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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexConfig;

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;

public class IndexCreatorImpl implements IndexCreator
{
    private final Collection<String> propertyKeys;
    private final Label[] labels;
    private final RelationshipType[] types;
    private final InternalSchemaActions actions;
    private final String indexName;
    private final IndexType indexType;
    private final IndexConfig indexConfig;

    public IndexCreatorImpl( InternalSchemaActions actions, Label... labels )
    {
        this( actions, labels, null, null, new ArrayList<>(), IndexType.BTREE, IndexConfig.empty() );
    }

    public IndexCreatorImpl( InternalSchemaActions actions, RelationshipType... types )
    {
        this( actions, null, types, null, new ArrayList<>(), IndexType.BTREE, IndexConfig.empty() );
    }

    private IndexCreatorImpl( InternalSchemaActions actions, Label[] labels, RelationshipType[] types, String indexName, Collection<String> propertyKeys,
            IndexType indexType, IndexConfig indexConfig )
    {
        this.actions = actions;
        this.labels = labels;
        this.types = types;
        this.indexName = indexName;
        this.propertyKeys = propertyKeys;
        this.indexType = indexType;
        this.indexConfig = indexConfig;

        assertInUnterminatedTransaction();
    }

    @Override
    public IndexCreator on( String propertyKey )
    {
        assertInUnterminatedTransaction();
        return new IndexCreatorImpl( actions, labels, types, indexName, copyAndAdd( propertyKeys, propertyKey ), indexType, indexConfig );
    }

    @Override
    public IndexCreator withName( String indexName )
    {
        assertInUnterminatedTransaction();
        return new IndexCreatorImpl( actions, labels, types, indexName, propertyKeys, indexType, indexConfig );
    }

    @Override
    public IndexCreator withIndexType( IndexType indexType )
    {
        assertInUnterminatedTransaction();
        return new IndexCreatorImpl( actions, labels, types, indexName, propertyKeys, indexType, indexConfig );
    }

    @Override
    public IndexCreator withIndexConfiguration( Map<IndexSetting,Object> indexConfiguration )
    {
        assertInUnterminatedTransaction();
        return new IndexCreatorImpl( actions, labels, types, indexName, propertyKeys, indexType, toIndexConfigFromIndexSettingObjectMap( indexConfiguration ) );
    }

    @Override
    public IndexDefinition create() throws ConstraintViolationException
    {
        assertInUnterminatedTransaction();

        if ( propertyKeys.isEmpty() )
        {
            throw new ConstraintViolationException( "An index needs at least one property key to index" );
        }

        if ( labels != null )
        {
            return actions.createIndexDefinition( labels, indexName, indexType, indexConfig, propertyKeys.toArray( new String[0] ) );
        }
        if ( types != null )
        {
            return actions.createIndexDefinition( types, indexName, indexType, indexConfig, propertyKeys.toArray( new String[0] ) );
        }
        throw new IllegalStateException( "Must have either labels or relationship types to create an index, but neither was present." );
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInOpenTransaction();
    }

    private Collection<String> copyAndAdd( Collection<String> propertyKeys, String propertyKey )
    {
        Collection<String> ret = new ArrayList<>( propertyKeys );
        ret.add( propertyKey );
        return ret;
    }
}
