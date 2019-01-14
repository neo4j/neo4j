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
package org.neo4j.kernel.impl.store.format;

import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
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

import static java.util.stream.Collectors.toSet;

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
    public String introductionVersion()
    {
        return actual.introductionVersion();
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
        Set<Capability> myCapabilities = Stream.of( actual.capabilities() ).collect( toSet() );
        myCapabilities.add( Capability.SECONDARY_RECORD_UNITS );
        return myCapabilities.toArray( new Capability[0] );
    }

    @Override
    public boolean hasCapability( Capability capability )
    {
        return capability == Capability.SECONDARY_RECORD_UNITS || actual.hasCapability( capability );
    }

    @Override
    public FormatFamily getFormatFamily()
    {
        return StandardFormatFamily.INSTANCE;
    }

    @Override
    public boolean hasCompatibleCapabilities( RecordFormats other, CapabilityType type )
    {
        return BaseRecordFormats.hasCompatibleCapabilities( this, other, type );
    }

    @Override
    public String name()
    {
        return this.getClass().getName();
    }
}
