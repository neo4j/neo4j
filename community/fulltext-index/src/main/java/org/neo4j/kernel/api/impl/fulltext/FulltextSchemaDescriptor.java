/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Properties;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.lock.ResourceType;

class FulltextSchemaDescriptor implements SchemaDescriptor
{
    private final SchemaDescriptor schema;
    private final Properties indexConfiguration;

    FulltextSchemaDescriptor( SchemaDescriptor schema, Properties indexConfiguration )
    {
        this.schema = schema;
        this.indexConfiguration = indexConfiguration;
    }

    @Override
    public boolean isAffected( long[] entityTokenIds )
    {
        return schema.isAffected( entityTokenIds );
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> computer )
    {
        return schema.computeWith( computer );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        schema.processWith( processor );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return schema.userDescription( tokenNameLookup );
    }

    @Override
    public int[] getPropertyIds()
    {
        return schema.getPropertyIds();
    }

    @Override
    public int getPropertyId()
    {
        return schema.getPropertyId();
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return schema.getEntityTokenIds();
    }

    @Override
    public int keyId()
    {
        return schema.keyId();
    }

    @Override
    public ResourceType keyType()
    {
        return schema.keyType();
    }

    @Override
    public EntityType entityType()
    {
        return schema.entityType();
    }

    @Override
    public SchemaDescriptor.PropertySchemaType propertySchemaType()
    {
        return schema.propertySchemaType();
    }

    @Override
    public SchemaDescriptor schema()
    {
        return this;
    }

    @Override
    public int hashCode()
    {
        return schema.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof FulltextSchemaDescriptor )
        {
            return schema.equals( ((FulltextSchemaDescriptor) obj).schema );
        }
        return schema.equals( obj );
    }

    Properties getIndexConfiguration()
    {
        return indexConfiguration;
    }

    boolean isEventuallyConsistent()
    {
        return Boolean.parseBoolean( indexConfiguration.getProperty( FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT ) );
    }
}
