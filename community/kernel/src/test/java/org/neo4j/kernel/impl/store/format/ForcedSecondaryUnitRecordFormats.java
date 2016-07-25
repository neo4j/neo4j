/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

/**
 * Wraps another {@link RecordFormats} and merely forces {@link AbstractBaseRecord#setSecondaryUnitId(long)}
 * to be used, and in extension {@link IdSequence#nextId()} to be called in
 * {@link RecordFormat#prepare(AbstractBaseRecord, int, IdSequence)}. All {@link RecordFormat} instances
 * will also be wrapped. This is a utility to test behavior when there are secondary record units at play.
 */
public class ForcedSecondaryUnitRecordFormats implements RecordFormats
{
    private final RecordFormats actual;

    public ForcedSecondaryUnitRecordFormats( RecordFormats actual )
    {
        this.actual = actual;
    }

    @Override
    public String storeVersion()
    {
        return actual.storeVersion();
    }

    @Override
    public int generation()
    {
        return actual.generation();
    }

    private static <R extends AbstractBaseRecord> RecordFormat<R> withForcedSecondaryUnit( RecordFormat<R> format )
    {
        return new ForcedSecondaryUnitRecordFormat<>( format );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return withForcedSecondaryUnit( actual.node() );
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return withForcedSecondaryUnit( actual.relationshipGroup() );
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return withForcedSecondaryUnit( actual.relationship() );
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return withForcedSecondaryUnit( actual.property() );
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken()
    {
        return withForcedSecondaryUnit( actual.labelToken() );
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return withForcedSecondaryUnit( actual.propertyKeyToken() );
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return withForcedSecondaryUnit( actual.relationshipTypeToken() );
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic()
    {
        return withForcedSecondaryUnit( actual.dynamic() );
    }

    @Override
    public RecordFormat<MetaDataRecord> metaData()
    {
        return withForcedSecondaryUnit( actual.metaData() );
    }

    @Override
    public Capability[] capabilities()
    {
        return actual.capabilities();
    }

    @Override
    public boolean hasCapability( Capability capability )
    {
        return actual.hasCapability( capability );
    }

    @Override
    public boolean hasSameCapabilities( RecordFormats other, CapabilityType type )
    {
        return actual.hasSameCapabilities( other, type );
    }
}
