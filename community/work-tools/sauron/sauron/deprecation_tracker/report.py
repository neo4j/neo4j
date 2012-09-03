from deprecation import list_deprecated_before
from docopt import docopt

from trello import TrelloClient
from datetime import datetime

CARD_DESC_TEMPLATE = '''Remove all items that were deprecated in or before version {tag}.
This list automatically updated: {date}

{deprecated_desc}

'''

def create_line_title(filename, line):
    for srcFolder in ['src/test/java/','src/main/java/']:
        if srcFolder in filename:
            return filename.split(srcFolder)[1].replace("/",".") + ":" + line
    return '{0}:{1}'.format(filename,line)

def report_in_trello(trello, board_id, list_id, tag, deprecated, github_baseurl):

    line_url = '{0}/blob/master'.format(github_baseurl)
    card_title = 'Remove deprecated methods from before {tag}'.format(tag=tag)

    # Create a markdown-formatted list of deprecated lines
    
    deprecated_desc = "* " + "\n* ".join( [ '[{title}]({line_url}/{file}#L{line})'.format(title=create_line_title(file,line), line_url=line_url, file=file, line=line) for file, line in deprecated] )

    desc = CARD_DESC_TEMPLATE.format(tag=tag,date=datetime.now(), deprecated_desc=deprecated_desc)

    # Find board
    board = trello.get_board(board_id)

    if len(deprecated) > 0:

       for card in board.open_cards():
           if card.name == card_title:
               print "Updating existing card.."
               card.set_description(desc)
               break
       else:
           # No card, create one
           trello.get_list(list_id).add_card(card_title, desc)

    else: # No deprecations
        for card in board.open_cards():
            if card.name == card_title:
                print "Archiving existing card (no deprecated things found).."
                card.set_closed(True)
                break

def report_deprecations(tag, trello, board_id, list_id, github_baseurl):
    # Get list of deprecated stuff
    print 'Finding @Deprecated tags added before {0}..'.format(tag)
    deprecated = list(list_deprecated_before(tag))

    # Post results to trello
    print 'Connecting to trello..'
    report_in_trello( trello, board_id, list_id, tag, deprecated, github_baseurl)

    print "Done."