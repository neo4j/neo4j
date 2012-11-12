
from trello import TrelloClient, ResourceUnavailable

MEMBER_CACHE = {}

def get_member_initials(trello, member_id):
    if not MEMBER_CACHE.has_key(member_id):
        MEMBER_CACHE[member_id] = trello.get_member(member_id)
    return MEMBER_CACHE[member_id].initials

def refresh_mordor_list(trello, mordor, list):
    mordor_list_name = '{board}: {list}'.format(board=list.board.name, list=list.name)
    print 'Refreshing "{list}"'.format(list=mordor_list_name)

    # Clear list if it already existed
    for mordor_list in mordor.all_lists():
        if mordor_list.name == mordor_list_name:
            for card in mordor_list.list_cards():
                card.delete()
            break
    else:
        mordor_list = mordor.add_list(mordor_list_name)

    for card in list.list_cards():
        members = ",".join([get_member_initials(trello, member_id) for member_id in card.member_ids])
        mordor_card = mordor_list.add_card(card.name + " [" + members + "]", card.url)

def refresh_mordor(trello, mordor_id, list_ids):

    mordor = trello.get_board(mordor_id)
    print 'Refresing Mordor board "{0}"'.format(mordor.name)

    for list_id in list_ids:
        refresh_mordor_list(trello, mordor, trello.get_list(list_id))