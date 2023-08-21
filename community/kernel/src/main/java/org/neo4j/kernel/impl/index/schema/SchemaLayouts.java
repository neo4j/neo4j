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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.index.internal.gbptree.RootLayerConfiguration.singleRoot;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.LayoutBootstrapper;
import org.neo4j.index.internal.gbptree.Meta;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.internal.id.indexed.IdRangeLayout;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsLayout;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

public class SchemaLayouts implements LayoutBootstrapper {
    private final List<LayoutBootstrapper> allSchemaLayout;

    public SchemaLayouts() {
        allSchemaLayout = Arrays.asList(
                idRangeLayout(),
                meta -> new Layouts(new TokenScanLayout(), singleRoot()),
                meta -> new Layouts(new IndexStatisticsLayout(), singleRoot()),
                rangeLayout(),
                meta -> new Layouts(
                        new PointLayout(IndexSpecificSpaceFillingCurveSettings.fromConfig(Config.defaults())),
                        singleRoot()));
    }

    public static String[] layoutDescriptions() {
        return new String[] {
            "Id range layout",
            "Token scan layout",
            "Index statistics layout",
            "Range index layout",
            "Point index layout"
        };
    }

    @Override
    public Layouts bootstrap(Meta meta) throws IOException {
        for (LayoutBootstrapper factory : allSchemaLayout) {
            final Layouts layout = factory.bootstrap(meta);
            if (layout != null && matchingLayout(meta, layout)) {
                return layout;
            }
        }
        throw new RuntimeException("Could not find any layout matching meta " + meta);
    }

    private static boolean matchingLayout(Meta meta, Layouts layouts) {
        try {
            meta.verify(layouts.dataLayout(), layouts.rootLayerConfiguration());
            return true;
        } catch (MetadataMismatchException e) {
            return false;
        }
    }

    private static LayoutBootstrapper rangeLayout() {
        return meta -> {
            int maxNumberOfSlots = 10;
            for (int numberOfSlots = 1; numberOfSlots < maxNumberOfSlots; numberOfSlots++) {
                var layouts = new Layouts(new RangeLayout(numberOfSlots), singleRoot());
                if (matchingLayout(meta, layouts)) {
                    return layouts;
                }
            }
            return null;
        };
    }

    private static LayoutBootstrapper idRangeLayout() {
        return meta -> {
            int maxExponent = 10;
            for (int exponent = 0; exponent < maxExponent; exponent++) {
                final int idsPerEntry = 1 << exponent;
                var layouts = new Layouts(new IdRangeLayout(idsPerEntry), singleRoot());
                if (matchingLayout(meta, layouts)) {
                    return layouts;
                }
            }
            return null;
        };
    }
}
