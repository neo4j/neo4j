package org.neo4j.helpers;

/**
 * ExtensionLoaders are used for runtime binding of interface instances.
 *
 */
public interface ExtensionLoader {
  <T> Iterable<T> loadExtensionsOfType( Class<T> type );
}
