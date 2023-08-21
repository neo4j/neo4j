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
package org.neo4j.configuration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.InternalLog;

/**
 * Config that delegates everything but also keeps track of any registered listeners through it
 * to be able to remove all the listeners belonging to this local config.
 */
public class LocalConfig extends Config {

    private final Config config;
    private Map<Setting<Object>, Collection<SettingChangeListener<Object>>> registeredListeners =
            new ConcurrentHashMap<>();

    public LocalConfig(Config config) {
        this.config = config;
    }

    @Override
    public <T> void addListener(Setting<T> setting, SettingChangeListener<T> listener) {
        registeredListeners
                .computeIfAbsent((SettingImpl<Object>) setting, v -> new ConcurrentLinkedQueue<>())
                .add((SettingChangeListener<Object>) listener);
        config.addListener(setting, listener);
    }

    @Override
    public <T> void removeListener(Setting<T> setting, SettingChangeListener<T> listener) {
        Collection<SettingChangeListener<Object>> listeners = registeredListeners.get(setting);
        if (listeners != null) {
            listeners.remove(listener);
        }
        config.removeListener(setting, listener);
    }

    public void removeAllLocalListeners() {
        for (var settingListeners : registeredListeners.entrySet()) {
            Setting<Object> setting = settingListeners.getKey();
            Collection<SettingChangeListener<Object>> listeners = settingListeners.getValue();
            for (SettingChangeListener<Object> listener : listeners) {
                config.removeListener(setting, listener);
            }
        }
        registeredListeners = new ConcurrentHashMap<>();
    }

    @Override
    public <T extends GroupSetting> Map<String, T> getGroups(Class<T> group) {
        return config.getGroups(group);
    }

    @Override
    public <T extends GroupSetting, U extends T> Map<Class<U>, Map<String, U>> getGroupsFromInheritance(
            Class<T> parentClass) {
        return config.getGroupsFromInheritance(parentClass);
    }

    @Override
    public <T> T get(Setting<T> setting) {
        return config.get(setting);
    }

    @Override
    public <T> T getDefault(Setting<T> setting) {
        return config.getDefault(setting);
    }

    @Override
    public <T> T getStartupValue(Setting<T> setting) {
        return config.getStartupValue(setting);
    }

    @Override
    public <T> ValueSource getValueSource(Setting<T> setting) {
        return config.getValueSource(setting);
    }

    @Override
    public <T> SettingObserver<T> getObserver(Setting<T> setting) {
        return config.getObserver(setting);
    }

    @Override
    public <T> void setDynamic(Setting<T> setting, T value, String scope) {
        config.setDynamic(setting, value, scope);
    }

    @Override
    public <T> void setDynamicByUser(Setting<T> setting, T value, String scope) {
        config.setDynamicByUser(setting, value, scope);
    }

    @Override
    public <T> void set(Setting<T> setting, T value) {
        config.set(setting, value);
    }

    @Override
    public <T> void setIfNotSet(Setting<T> setting, T value) {
        config.setIfNotSet(setting, value);
    }

    @Override
    public boolean isExplicitlySet(Setting<?> setting) {
        return config.isExplicitlySet(setting);
    }

    @Override
    public String toString() {
        return config.toString();
    }

    @Override
    public String toString(boolean includeNullValues) {
        return config.toString(includeNullValues);
    }

    @Override
    public void setLogger(InternalLog log) {
        config.setLogger(log);
    }

    @Override
    public Setting<Object> getSetting(String name) {
        return config.getSetting(name);
    }

    @Override
    public Map<String, Setting<Object>> getDeclaredSettings() {
        return config.getDeclaredSettings();
    }

    @Override
    public Object configStringLookup(String setting) {
        return config.configStringLookup(setting);
    }
}
