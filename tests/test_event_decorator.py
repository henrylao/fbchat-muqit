from fbchat_muqit.events.dispatcher import EventDispatcher, EventType


def test_event_decorator_autodetect_from_function_name():
    dispatcher = EventDispatcher()

    @dispatcher.event
    async def on_message(message):
        return

    assert EventType.MESSAGE in dispatcher._event_listeners
    assert on_message in dispatcher._event_listeners[EventType.MESSAGE]


def test_event_decorator_explicit_event_type():
    dispatcher = EventDispatcher()

    @dispatcher.event(EventType.PRESENCE)
    async def on_presence(event):
        return

    assert EventType.PRESENCE in dispatcher._event_listeners
    assert on_presence in dispatcher._event_listeners[EventType.PRESENCE]
