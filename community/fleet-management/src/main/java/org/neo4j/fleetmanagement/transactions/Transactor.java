package org.neo4j.fleetmanagement.transactions;

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

import java.util.List;
import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.model.Database;
import org.neo4j.fleetmanagement.topology.model.GraphCount;
import org.neo4j.fleetmanagement.topology.model.Server;
import org.neo4j.fleetmanagement.transactions.editions.CommunityTransactor;
import org.neo4j.fleetmanagement.transactions.editions.EnterpriseTransactor;
import org.neo4j.fleetmanagement.transactions.model.VersionAndEdition;
import org.neo4j.kernel.impl.factory.DbmsInfo;

@ServiceProvider
public class Transactor implements ITransactor {
    private final ITransactor editionTransactor;

    public Transactor() {
        this.editionTransactor = null;
    }

    public Transactor(DbmsInfo dbmsInfo, ServerIdentity serverIdentity, State state) {
        if (dbmsInfo == null) {
            throw new IllegalArgumentException("dbmsInfo cannot be null");
        }

        if (dbmsInfo.equals(DbmsInfo.ENTERPRISE)) {
            editionTransactor = new EnterpriseTransactor(state);
        } else if (dbmsInfo.equals(DbmsInfo.COMMUNITY)) {
            editionTransactor = new CommunityTransactor(serverIdentity, state);
        } else {
            throw new IllegalArgumentException("unsupported dbms edition: " + dbmsInfo);
        }
    }

    @Override
    public void init(DatabaseManagementService databaseManagementService) {
        this.editionTransactor.init(databaseManagementService);
    }

    @Override
    public boolean getTokenStatus() {
        return this.editionTransactor.getTokenStatus();
    }

    @Override
    public boolean getTokenRotationStatus() {
        return this.editionTransactor.getTokenRotationStatus();
    }

    @Override
    public String getToken() {
        return this.editionTransactor.getToken();
    }

    @Override
    public VersionAndEdition getVersionAndEdition() {
        return this.editionTransactor.getVersionAndEdition();
    }

    @Override
    public Map<String, Server> getServers() {
        return this.editionTransactor.getServers();
    }

    @Override
    public Map<String, List<Database>> getDatabases() {
        return this.editionTransactor.getDatabases();
    }

    @Override
    public Server.License getLicense() {
        return this.editionTransactor.getLicense();
    }

    @Override
    public Server.License getGdsLicense() {
        return this.editionTransactor.getGdsLicense();
    }

    @Override
    public Server.License getBloomLicense() {
        return this.editionTransactor.getBloomLicense();
    }

    @Override
    public GraphCount getGraphCount(String databaseName) {
        return this.editionTransactor.getGraphCount(databaseName);
    }

    @Override
    public void setToken(String token) {
        this.editionTransactor.setToken(token);
    }

    @Override
    public void rotateToken() {
        assert this.editionTransactor != null;
        this.editionTransactor.rotateToken();
    }

    @Override
    public void deleteToken() {
        assert this.editionTransactor != null;
        this.editionTransactor.deleteToken();
    }
}
