/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Configuration parameters for the bloom fulltext addon.
 */
public class BloomFulltextConfig implements LoadableConfig
{
    @Description( "Enable the fulltext addon for bloom." )
    @Internal
    static final Setting<Boolean> bloom_enabled = setting( "unsupported.dbms.bloom_enabled", BOOLEAN, FALSE );

    @Description( "Define the analyzer to use for the bloom index. Expects the fully qualified classname of the " +
                  "analyzer to use" )
    @Internal
    static final Setting<String> bloom_default_analyzer = setting( "unsupported.dbms.bloom_default_analyzer", STRING,
            "org.apache.lucene.analysis.standard.StandardAnalyzer" );
}
