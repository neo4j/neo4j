package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.util.List;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.setting;

public class LoadableBloomFulltextConfig implements LoadableConfig
{

    // Bloom index
    @Internal
    public static final Setting<List<String>> bloom_indexed_properties = setting( "unsupported.dbms.bloom_indexed_properties", STRING_LIST, NO_DEFAULT );

    @Description( "Define the analyzer to use for the bloom index. Expects the fully qualified classname of the analyzer to use" )
    @Internal
    public static final Setting<String> bloom_analyzer =
            setting( "unsupported.dbms.bloom_analyzer", STRING, "org.apache.lucene.analysis.standard.StandardAnalyzer" );

}
