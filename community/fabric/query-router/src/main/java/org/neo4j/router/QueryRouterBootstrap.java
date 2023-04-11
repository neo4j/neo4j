/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.router;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;

public class QueryRouterBootstrap {

    public static class Community extends FabricServicesBootstrap.Community {

        public Community(
                LifeSupport lifeSupport,
                Dependencies dependencies,
                LogService logService,
                DatabaseContextProvider<? extends DatabaseContext> databaseProvider,
                DatabaseReferenceRepository databaseReferenceRepo) {
            super(lifeSupport, dependencies, logService, databaseProvider, databaseReferenceRepo);
        }
    }
}
