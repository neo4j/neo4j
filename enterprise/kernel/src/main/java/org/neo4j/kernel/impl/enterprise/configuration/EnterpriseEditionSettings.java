package org.neo4j.kernel.impl.enterprise.configuration;


import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Settings;

/**
 * Enterprise edition specific settings
 */
public class EnterpriseEditionSettings
{
    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'RELATIONSHIP' type is supported. " )
    public static Setting<List<String>> idTypesToReuse =
            Settings.setting( "dbms.ids.reuse.types.override", Settings.STRING_LIST, "" );

}
