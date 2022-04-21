/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.Value;

class GenericNativeIndexAccessor extends NativeIndexAccessor<BtreeKey> {
    private final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;
    private final TokenNameLookup tokenNameLookup;
    private IndexValueValidator validator;

    GenericNativeIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<BtreeKey> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IndexDescriptor descriptor,
            IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings,
            SpaceFillingCurveConfiguration configuration,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions) {
        super(databaseIndexContext, indexFiles, layout, descriptor, openOptions);
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
        this.tokenNameLookup = tokenNameLookup;
        instantiateTree(recoveryCleanupWorkCollector, headerWriter);
    }

    @Override
    protected void afterTreeInstantiation(GBPTree<BtreeKey, NullValue> tree) {
        validator = new GenericIndexKeyValidator(tree.keyValueSizeCap(), descriptor, layout, tokenNameLookup);
    }

    @Override
    public ValueIndexReader newValueReader() {
        assertOpen();
        return new GenericNativeIndexReader(tree, layout, descriptor, spaceFillingCurveSettings, configuration);
    }

    @Override
    public void validateBeforeCommit(long entityId, Value[] tuple) {
        validator.validate(entityId, tuple);
    }

    @Override
    public Map<String, Value> indexConfig() {
        Map<String, Value> map = new HashMap<>();
        spaceFillingCurveSettings.visitIndexSpecificSettings(new SpatialConfigVisitor(map));
        return map;
    }
}
