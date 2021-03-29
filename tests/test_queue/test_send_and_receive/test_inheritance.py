from platonic.sqs.queue import SQSReceiver, SQSSender


class Spell:
    """Magical spell."""

    def __init__(self, text: str) -> None:
        """Initialize."""
        self.text = text


class SpellSender(SQSSender[Spell]):
    """Spell sender."""

    def serialize_value(self, spell: Spell) -> str:
        """Serialize a spell."""
        return spell.text


class SpellReceiver(SQSReceiver[Spell]):
    """Spell receiver."""

    def deserialize_value(self, raw_value: str) -> Spell:
        """Deserialize a spell."""
        return Spell(text=raw_value)


def test_send_and_receive_int(sqs_queue_url: str):
    """Send a message and get it back."""
    sender = SpellSender(url=sqs_queue_url)
    receiver = SpellReceiver(url=sqs_queue_url)

    spell = Spell(text='Ashonai')

    sender.send(spell)

    assert receiver.receive().value.text == 'Ashonai'
