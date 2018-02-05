package org.neo4j.kernel.api.schema;

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.util.stream.Collectors.joining;

public class NonSchemaSchemaDescriptor implements org.neo4j.internal.kernel.api.schema.NonSchemaSchemaDescriptor
{
    private final int[] entityTokens;
    private final EntityType entityType;
    private final int[] propertyIds;

    public NonSchemaSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds )
    {
        this.entityTokens = entityTokens;
        this.entityType = entityType;
        this.propertyIds = propertyIds;
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> computer )
    {
        return computer.computeSpecific( this );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        processor.processSpecific( this );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return String.format( entityType + ":%s(%s)",
                Arrays.stream( tokenNameLookup.entityTokensGetNames( entityType, entityTokens ) ).collect( joining( ", " ) ),
                SchemaUtil.niceProperties( tokenNameLookup, propertyIds ) );
    }

    @Override
    public int[] getPropertyIds()
    {
        return propertyIds;
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return entityTokens;
    }

    @Override
    public int keyId()
    {
        //TODO figure out locking
        return 1;
    }

    @Override
    public ResourceType keyType()
    {
        if ( entityType == EntityType.NODE )
        {
            return ResourceTypes.LABEL;
        }
        else if ( entityType == EntityType.RELATIONSHIP )
        {
            return ResourceTypes.RELATIONSHIP_TYPE;
        }
        throw new UnsupportedOperationException( "Keys for non-schema indexes of type " + entityType + " is not supported." );
    }

    @Override
    public EntityType entityType()
    {
        return entityType;
    }

    @Override
    public PropertySchemaType propertySchemaType()
    {
        return PropertySchemaType.NON_SCHEMA_ANY;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return this;
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
        NonSchemaSchemaDescriptor that = (NonSchemaSchemaDescriptor) o;
        return Arrays.equals( entityTokens, that.entityTokens ) && entityType == that.entityType && Arrays.equals( propertyIds, that.propertyIds );
    }

    @Override
    public int hashCode()
    {

        int result = Objects.hash( entityType );
        result = 31 * result + Arrays.hashCode( entityTokens );
        result = 31 * result + Arrays.hashCode( propertyIds );
        return result;
    }
}
