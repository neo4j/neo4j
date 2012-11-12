#!/usr/bin/python
""" Sauron.

Megalomaniac ruler of the underworld. Also, utility bot for automating basic engineering tasks. Breaks all rules about
SRP and unix command line tools.

Credentials:
  Login to trello can either be provided via the command line, or in a config file in ~/.sauron

  It should look something like:
  trello_api_key: 'API_KEY_GOES_HERE'
  trello_user_token: 'TOKEN_FOR_USER_ACCOUNT_TO_USE'

Capabilities:

  Tracking deprecation:
    Updates a trello ticket with a list of deprecated things that should be removed. Picks up the current version
    from pom.xml from the directory sauron is invoked in, and checks for @Deprecated things in the repository that
    were added more than <versions_back> before the current version.

  Maintaining mordor:
    Tracks a set of trello lists, and maintains a single board showing all cards that are those lists. This
    can be used to get an overview, for example, of what is "in dev" across multiple trello boards.
    
    You can either specify what board to use as mordor and what lists to track directly on the command line,
    or in the config file. If you specify in the config file, you can just call sauron mordor.
    
    Configuration should look like (~/.sauron):
    
    mordor:
        board: 'MORDOR_BOARD_ID'
        lists:
        [
          'LIST_ID'
          'LIST_ID'
          'LIST_ID'
        ]

  Trello maintenence:
    Currently some basic tasks to list boards and lists via command line. Can be used to get list ids to use for
    maintaining mordor.



Usage:
  sauron mordor
  sauron mordor <mordor_board_id> from <track_list_id>... [options]
  sauron trello boards [options]
  sauron trello lists [<board_id>] [options]
  sauron deprecations <github_repo_url> <board_id> <list_id>
  sauron deprecations before <git_tag>
  sauron deprecations after <git_tag>
  sauron deprecations removed between <one_git_tag> and <second_git_tag>


Options:
  -h --help                       Show this screen.
  --version                       Show version.
  --trellokey=<trello_key>        Trello API key for this script [Default: None]
  --trellotoken=<trello_token>    Trello read/write token for the user to act as [Default: None]

"""
from docopt import docopt

from sauron.mordor import refresh_mordor
from sauron.trello import list_boards, list_lists
from sauron.deprecation_tracker import update_deprecated_card
from sauron.deprecation_tracker.deprecation import list_deprecated_before, list_deprecated_after, list_removed_deprecations_between

from trello import TrelloClient

def load_config():
    import os
    from os.path import isfile
    if isfile(os.getenv("HOME") + '/.sauron'):
        from config import Config
        return Config(file(os.getenv("HOME") + '/.sauron'))
    else:
        return None


def main():

    args = docopt(__doc__, version='Mordor 1.0')

    trello_key =    args['--trellokey']
    trello_token =  args['--trellotoken']

    config = load_config()

    if trello_key == 'None' or trello_token == 'None':
        if config is None:
            print "Please provide trello token and key options (via config or command line, see usage) if you want me to connect to Trello."
            exit(1)
        else:
            trello_key = config.trello_api_key
            trello_token = config.trello_user_token

    trello = TrelloClient(trello_key, trello_token)


    # TODO: Replace below with some interface that commands can implement, and make this
    # look up commands rather than use this giant if-tree
    if args['trello']:
        if args['boards']:
            list_boards(trello)
        elif args['lists']:
            if args.has_key('<board_id>'):
                list_lists(trello, args['<board_id>'])
            else:
                list_lists(trello)

    elif args['mordor']:
        if args.has_key('<mordor_board_id>') and args['<mordor_board_id>'] != None:
            mordor_board = args['<mordor_board_id>']
            track_lists = args['<track_list_id>']
        elif hasattr(config, 'mordor'):
            mordor_board = config.mordor.board
            track_lists = config.mordor.lists
        else:
            print "Please provide mordor config (mordor board and lists to track) either on the command line or in .mordor config file"
            exit(1)
            
        refresh_mordor(trello, mordor_board, track_lists)

    elif args['deprecations']:
        if args['after']:
            for filename, line in list_deprecated_after(args['<git_tag>']):
                print '{0}:{1}'.format(filename, line)
        elif args['before']:
            for filename, line in list_deprecated_before(args['<git_tag>']):
                print '{0}:{1}'.format(filename, line)
        elif args['removed']:
            list_removed_deprecations_between(args['<one_git_tag>'], args['<second_git_tag>'])
        else:
            update_deprecated_card(trello, 2, args['<github_repo_url>'], args['<board_id>'], args['<list_id>'])

if __name__ == '__main__':
    main()
