
from trello import TrelloClient, ResourceUnavailable

def list_boards(trello):
    for board in trello.list_boards():
        print '{id}\t{name}'.format(name=board.name,id=board.id)

def list_lists(trello, board_id=None):
    if board_id is None:
        for board in trello.list_boards():
            for lst in trello.get_board(board.id).all_lists():
                print '{id}\t{board}/{name}'.format(board=board.name, name=lst.name,id=lst.id)
    else:
        for lst in trello.get_board(board_id).all_lists():
            print '{id}\t{name}'.format(name=lst.name,id=lst.id)