VERSION := $(shell bin/extract-version-from-pom pom.xml || { echo >&2 "Error reading version from pom (perhaps xmlstarlet is not installed), aborting"; kill $$PPID; })

ZIP_DISTRIBUTION := cypher-shell/target/cypher-shell-$(VERSION).zip
EXTRACTED := tmp/install
TYRE_KICKING_TMP := tmp/tyre-kicking

$(ZIP_DISTRIBUTION): cypher-shell/src cypher-shell/pom.xml
	cd ../.. && mvn package --projects org.neo4j:cypher-shell --also-make -DskipTests

$(EXTRACTED): $(ZIP_DISTRIBUTION)
	rm -fr $(EXTRACTED)
	mkdir -p $(EXTRACTED)
	unzip $(ZIP_DISTRIBUTION) -d $(EXTRACTED)

run: $(EXTRACTED) ## Run Cypher Shell
	$(EXTRACTED)/cypher-shell/cypher-shell
.PHONY: run

tyre-kicking-test: $(ZIP_DISTRIBUTION) tyre-kicking-test/tyrekicking.sh
	rm -fr $(TYRE_KICKING_TMP)
	tyre-kicking-test/tyrekicking.sh $(TYRE_KICKING_TMP) $(ZIP_DISTRIBUTION)
.PHONY: tyre-kicking-test
