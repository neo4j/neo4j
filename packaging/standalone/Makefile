.PHONY: help
script_names=neo4j neo4j-admin neo4j-backup neo4j-import
scripts=$(patsubst %,src/main/distribution/shell-scripts/bin/%,${script_names})
help: ## Print this help text
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

generate-scripts: ${scripts} ## Build shell scripts using m4
		
%: %.m4 src/main/distribution/shell-scripts/bin/neo4j-shared.m4
	m4 $< > $@
	chmod 0755 $@	
