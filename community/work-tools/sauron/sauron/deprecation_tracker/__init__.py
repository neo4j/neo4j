from report import report_deprecations

from sauron.maven.versions import determine_project_version, subtract_version

def update_deprecated_card(trello, versions_back, github_repo_url, board_id, list_id):

    version = determine_project_version()
    deprecation_version = subtract_version(version,versions_back)

    print 'Determined current version: {version}'.format(version=version)

    report_deprecations(deprecation_version, trello, board_id, list_id, github_repo_url)