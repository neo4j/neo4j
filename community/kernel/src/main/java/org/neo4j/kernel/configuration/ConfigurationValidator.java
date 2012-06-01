package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.Pair;

/**
 * Given a set of annotated config classes,
 * validates configuration maps using the validators
 * in the setting class fields.
 */
public class ConfigurationValidator {

	private AnnotatedFieldHarvester fieldHarvester = new AnnotatedFieldHarvester();
	private Map<String, GraphDatabaseSetting<?>> settings;
	
	public ConfigurationValidator(Iterable<Class<?>> settingsClasses)
	{
		this.settings = getSettingsFrom(settingsClasses);
	}
	
	public void validate(Map<String,String> rawConfig)
	{
		for(String key : rawConfig.keySet()) 
		{
			if(settings.containsKey(key))
			{
				settings.get(key).validate(rawConfig.get(key));
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private Map<String, GraphDatabaseSetting<?>> getSettingsFrom(Iterable<Class<?>> settingsClasses) 
	{
		Map<String, GraphDatabaseSetting<?>> settings = new HashMap<String, GraphDatabaseSetting<?>>();
		for(Class<?> clazz : settingsClasses)
		{
			for(Pair<Field, GraphDatabaseSetting> field : fieldHarvester.findStatic(clazz, GraphDatabaseSetting.class))
			{
				settings.put(field.other().name(), field.other());
			}
		}
		return settings;
	}
	
}
