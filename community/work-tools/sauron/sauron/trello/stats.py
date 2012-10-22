from trello import TrelloClient, ResourceUnavailable

def gather_trello_stats(trello, board_id):
    for card in trello.get_board(board_id).all_cards():
        resp = card.client.fetch_json(
            '/cards/'+card.id,
            query_params = {'badges': False})

        print resp
        print repr(resp)

        print '{name}'.format(name=card.name)