"""The On Store logo: a hand of playing cards.

    ╭───╮╭───╮  ╭───╮╭───╮╭───╮╭───╮╭───╮
    │♠  ││♥  │  │♦  ││♣  ││♠  ││♥  ││♦  │
    │ O ││ N │  │ S ││ T ││ O ││ R ││ E │
    │  ♠││  ♥│  │  ♦││  ♣││  ♠││  ♥││  ♦│
    ╰───╯╰───╯  ╰───╯╰───╯╰───╯╰───╯╰───╯

Every screen carries one suit in its badge, and the suit is who you are:

    ♠ spades    the admin (the ace of spades: the top card)
    ♥ hearts    a client
    ♦ diamonds  the sign-in portal (where keys, the valuables, are handed out)
    ♣ clubs     the flow screen (the cluster at work)

Nothing says so on screen. A hidden command, `:deal`, deals cards from a
shuffled deck, each with the logo on it. The admin's first card is always the
ace of spades, a client's the ace of hearts. Somewhere in the deck there is a
joker that tells what the suits mean.
"""
import random

from rich.text import Text

SPADES, HEARTS, DIAMONDS, CLUBS = "♠", "♥", "♦", "♣"
SUITS = (SPADES, HEARTS, DIAMONDS, CLUBS)
RANKS = ("A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K")
SUIT_OF = {"admin": SPADES, "client": HEARTS, "portal": DIAMONDS, "flow": CLUBS}
SUIT_NAMES = {SPADES: "spades", HEARTS: "hearts", DIAMONDS: "diamonds", CLUBS: "clubs"}
JOKER = ("JOKER", "★")
ACE_OF_SPADES, ACE_OF_HEARTS = ("A", SPADES), ("A", HEARTS)

RED, BLACK_ON_DARK, EDGE, FACE = "#ff3b5c", "#e8e8e8", "#5f5f5f", "bold #ffffff"
BADGE_BG = "#00e5ff"


def is_red(suit: str) -> bool:
    return suit in (HEARTS, DIAMONDS)


def suit_color(suit: str) -> str:
    return RED if is_red(suit) else BLACK_ON_DARK


def badge(suit: str = SPADES) -> Text:
    """The top-bar badge: ' ♠ ON STORE ', the suit in card colours."""
    t = Text(" ", style=f"on {BADGE_BG}")
    t.append(suit, style=f"bold {'#b00020' if is_red(suit) else '#000000'} on {BADGE_BG}")
    t.append(" ON STORE ", style=f"bold #000000 on {BADGE_BG}")
    return t


def wordmark() -> Text:
    """The logo: ON STORE as a hand of seven cards, suits going round."""
    words = ["ON", "STORE"]
    cards = []                                   # (letter, suit) or None for the gap between words
    n = 0
    for w, word in enumerate(words):
        if w:
            cards.append(None)
        for letter in word:
            cards.append((letter, SUITS[n % 4]))
            n += 1
    rows = [Text() for _ in range(5)]
    for card in cards:
        if card is None:
            for r in rows:
                r.append("  ")
            continue
        letter, suit = card
        c = suit_color(suit)
        rows[0].append("╭───╮", style=EDGE)
        rows[1].append("│", style=EDGE)
        rows[1].append(suit, style=c)
        rows[1].append("  ")
        rows[1].append("│", style=EDGE)
        rows[2].append("│ ", style=EDGE)
        rows[2].append(letter, style=FACE)
        rows[2].append(" │", style=EDGE)
        rows[3].append("│  ", style=EDGE)
        rows[3].append(suit, style=c)
        rows[3].append("│", style=EDGE)
        rows[4].append("╰───╯", style=EDGE)
    out = Text(justify="center")
    for i, r in enumerate(rows):
        out.append_text(r)
        if i < len(rows) - 1:
            out.append("\n")
    return out


def card_art(rank: str, suit: str) -> Text:
    """One card, 15 wide, with the On Store logo on its face."""
    w = 13                                       # inside the border
    joker = (rank, suit) == JOKER
    c = "#d787ff" if joker else suit_color(suit)
    t = Text()

    def line(content: Text = None) -> None:
        t.append("│", style=EDGE)
        body = content or Text(" " * w)
        body.pad_right(w - body.cell_len)
        t.append_text(body)
        t.append("│\n", style=EDGE)

    def at(text: str, style: str, align: str = "left") -> Text:
        pad = w - 2 - len(text)
        return Text(" " + (" " * pad if align == "right" else "") + text
                    + ("" if align == "right" else " " * pad) + " ", style=style)

    def center(text: str, style: str) -> Text:
        left = (w - len(text)) // 2
        return Text(" " * left + text + " " * (w - len(text) - left), style=style)

    t.append("╭" + "─" * w + "╮\n", style=EDGE)
    line(at("J" if joker else rank, f"bold {c}"))
    line(at(suit, c))
    line()
    line(center(suit if not joker else "JOKER", f"bold {c}"))
    line(center("ON STORE", FACE))
    line()
    line(at(suit, c, "right"))
    line(at("J" if joker else rank, f"bold {c}", "right"))
    t.append("╰" + "─" * w + "╯", style=EDGE)
    return t


class Deck:
    """52 cards and one joker, shuffled; `top` (rank, suit) is dealt first.
    When it runs out it is shuffled again."""

    def __init__(self, top: tuple = None, rng: random.Random = None):
        self.rng = rng or random.Random()
        self.reshuffled = 0
        self._fill(top)

    def _fill(self, top=None) -> None:
        self.cards = [(r, s) for s in SUITS for r in RANKS] + [JOKER]
        self.rng.shuffle(self.cards)
        if top in self.cards:
            self.cards.remove(top)
            self.cards.append(top)               # deal() takes from the end

    def deal(self) -> tuple:
        if not self.cards:
            self.reshuffled += 1
            self._fill()
        return self.cards.pop()

    def __len__(self) -> int:
        return len(self.cards)


def describe(card: tuple) -> str:
    """'the ace of spades', or what the joker has to say."""
    if card == JOKER:
        return ("the JOKER. Psst: the suits are not decoration. "
                "♠ is the admin, ♥ a client, ♦ the door where keys are handed out, ♣ the flow of the cluster.")
    rank, suit = card
    name = {"A": "ace", "J": "jack", "Q": "queen", "K": "king"}.get(rank, rank)
    return f"the {name} of {SUIT_NAMES[suit]}"


def deal_text(deck: Deck, who: str = "") -> Text:
    """A dealt card with a line saying what it is: for the output pane."""
    card = deck.deal()
    t = Text()
    if deck.reshuffled and len(deck) == 52:
        t.append("the deck ran out: shuffled again\n", style="#6c6c6c")
    t.append_text(card_art(*card))
    t.append("\n")
    t.append(f"{who + ' is' if who else 'you are'} dealt {describe(card)}", style="#d787ff" if card == JOKER
             else suit_color(card[1]))
    return t
