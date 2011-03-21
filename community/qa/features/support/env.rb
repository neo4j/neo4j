$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../../lib')

require 'features/support/shell_helpers'
require 'features/support/network_helpers'
require 'features/support/platform_helpers'
require 'features/support/neo4j_helpers'

World(ShellHelpers, NetworkHelpers, PlatformHelpers, Neo4JHelpers)

