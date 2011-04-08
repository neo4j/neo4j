module Neo4JHelpers

  attr_accessor :neo4j

  def neo4j
    unless (@neo4j)
      @neo4j= Neo4jEnvironment.new
    end
    @neo4j
  end

  def archive_name
    "neo4j-" + @neo4j.product + "-" + @neo4j.version + "-" + current_platform.type + "."+current_platform.extension
  end

  class Neo4jEnvironment
    attr_accessor :version, :download_location, :home, :product
    attr_accessor :version, :download_host, :home, :product
  end
end
