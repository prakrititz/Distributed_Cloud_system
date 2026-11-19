"""The On Store logo (dashboard/logo.py): a hand of cards, a suit per screen,
and the hidden :deal command."""
import random

from dashboard.logo import (ACE_OF_HEARTS, ACE_OF_SPADES, JOKER, SUIT_OF, Deck, badge, card_art, deal_text,
                            describe, wordmark)


def test_the_wordmark_spells_on_store_in_cards():
    lines = wordmark().plain.split("\n")
    assert len(lines) == 5 and len({len(l) for l in lines}) == 1         # a neat block
    assert lines[2].replace("│", "").split() == list("ONSTORE")
    assert "".join(c for c in lines[1] if c in "♠♥♦♣") == "♠♥♦♣♠♥♦"


def test_each_screen_has_its_own_suit():
    assert len(set(SUIT_OF.values())) == 4
    for suit in SUIT_OF.values():
        assert badge(suit).plain == f" {suit} ON STORE "


def test_a_card_is_a_neat_rectangle_with_the_logo():
    for card in [("A", "♠"), ("10", "♥"), JOKER]:
        lines = card_art(*card).plain.split("\n")
        assert len(lines) == 10 and {len(l) for l in lines} == {15}
        assert any("ON STORE" in l for l in lines)


def test_the_deck_deals_every_card_once_then_shuffles_again():
    deck = Deck(top=ACE_OF_SPADES, rng=random.Random(1))
    first = deck.deal()
    dealt = {first} | {deck.deal() for _ in range(52)}
    assert first == ACE_OF_SPADES and len(dealt) == 53 and JOKER in dealt
    deck.deal()                                                           # empty: a fresh deck
    assert deck.reshuffled == 1 and len(deck) == 52


def test_the_joker_tells_the_secret():
    assert "♠ is the admin" in describe(JOKER) and describe(ACE_OF_HEARTS) == "the ace of hearts"
    assert "dealt the ace of hearts" in deal_text(Deck(top=ACE_OF_HEARTS)).plain
