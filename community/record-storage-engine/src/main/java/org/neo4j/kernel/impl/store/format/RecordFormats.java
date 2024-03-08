/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.service.NamedService;
import org.neo4j.storageengine.api.StoreFormatLimits;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * The record formats that a store version uses. Contains all formats for all different stores as well as
 * accessors for which {@link Capability capabilities} a format has as to be able to compare between formats.
 */
public interface RecordFormats {
    int NO_GENERATION = -1;

    @Service
    interface Factory extends NamedService {
        RecordFormats getInstance();
    }

    /**
     * @return the neo4j version where this format was introduced. It is almost certainly NOT the only version of
     * neo4j where this format is used.
     */
    String introductionVersion();

    /**
     * Generation of this format, format family local int value which should be incrementing along with
     * releases, e.g. store version, e.g. official versions of the product. Used to determine generation of particular
     * format and to be able to find newest of among them.
     * When implementing new format generation can be assigned to any positive integer, but please take into account
     * future version generations.
     * When evolving an older format the generation of the new format version should
     * be higher than the format it evolves from.
     * The generation value doesn't need to correlate to any other value, the only thing needed is to
     * determine "older" or "newer".
     *
     * @return format generation, with the intent of usage being that a store can migrate to a newer or
     * same generation, but not to an older generation within same format family.
     * May return {@link #NO_GENERATION} meaning that it should not be considered for succession etc.
     * (useful for marking test-only formats with).
     */
    int majorVersion();

    int minorVersion();

    RecordFormat<NodeRecord> node();

    RecordFormat<RelationshipGroupRecord> relationshipGroup();

    RecordFormat<RelationshipRecord> relationship();

    RecordFormat<PropertyRecord> property();

    RecordFormat<SchemaRecord> schema();

    RecordFormat<LabelTokenRecord> labelToken();

    RecordFormat<PropertyKeyTokenRecord> propertyKeyToken();

    RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken();

    RecordFormat<DynamicRecord> dynamic();

    RecordFormat<MetaDataRecord> metaData();

    /**
     * Use when comparing one format to another, for example for migration purposes.
     *
     * @return array of {@link Capability capabilities} for comparison.
     */
    Capability[] capabilities();

    /**
     * @param capability {@link Capability} to check for.
     * @return whether or not this format has a certain {@link Capability}.
     */
    boolean hasCapability(Capability capability);

    /**
     * Get format family to which this format belongs to.
     * @return format family
     * @see FormatFamily
     */
    FormatFamily getFormatFamily();

    /**
     * Whether or not changes in the {@code other} format, compared to this format, for the given {@code type}, are compatible.
     *
     * @param other {@link RecordFormats} to check compatibility with.
     * @param type {@link CapabilityType type} of capability to check compatibility for.
     * @return true if the {@code other} format have compatible capabilities of the given {@code type}.
     */
    boolean hasCompatibleCapabilities(RecordFormats other, CapabilityType type);

    /**
     * Record format name
     * @return name of record format
     */
    String name();

    /**
     * Can be used while developing a new format make sure it has a higher generation than the real formats and that
     * {@link GraphDatabaseInternalSettings#include_versions_under_development} is set.
     */
    default boolean formatUnderDevelopment() {
        return false;
    }

    /**
     * Some formats we just keep track of to be able to migrate from them.
     * A format that is only for migration is not supported to run a database on, and does not have support in all tools.
     */
    boolean onlyForMigration();

    /**
     * Id limits for the store format.
     */
    StoreFormatLimits idLimits();
}
