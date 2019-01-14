/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.security.enterprise.diagnostics;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsRotatingFile;

public class SecurityLogDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private Config config;

    public SecurityLogDiagnosticsOfflineReportProvider()
    {
        super( "enterprise-security", "logs" );
    }

    @Override
    public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
    {
        this.fs = fs;
        this.config = config;
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        if ( classifiers.contains( "logs" ) )
        {
            File securityLog = config.get( SecuritySettings.security_log_filename );
            if ( fs.fileExists( securityLog ) )
            {
                return newDiagnosticsRotatingFile( "logs/security.log", fs, securityLog );
            }
        }
        return Collections.emptyList();
    }
}
