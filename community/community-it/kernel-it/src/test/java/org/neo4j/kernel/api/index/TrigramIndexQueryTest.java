package org.neo4j.kernel.api.index;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;

public class TrigramIndexQueryTest extends TextIndexQueryTest {
    @Override
    public ReadTestSupport newTestSupport() {
        var readTestSupport = super.newTestSupport();
        readTestSupport.addSetting(GraphDatabaseInternalSettings.trigram_index, true);
        return readTestSupport;
    }

    @Override
    protected IndexProviderDescriptor getIndexProviderDescriptor() {
        return TrigramIndexProvider.DESCRIPTOR;
    }
}
