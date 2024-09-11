# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import logging
from dataclasses import dataclass
from typing import Any

from frequenz.channels import Receiver, Sender
from frequenz.client.dispatch.types import ComponentSelector
from frequenz.sdk.actor import Actor

from ._dispatch import Dispatch, RunningState

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class DispatchConfigurationEvent:
    """Event emitted when the dispatch configuration changes."""

    components: ComponentSelector
    """Components to be used."""

    dry_run: bool
    """Whether this is a dry run."""

    payload: dict[str, Any]
    """Additional payload."""


class DispatchRunnerActor(Actor):
    """Helper class to manage actors based on dispatches.

    Example usage:

    ```python
    import os
    import asyncio
    from frequenz.dispatch import Dispatcher, DispatchRunnerActor, DispatchConfigurationEvent
    from frequenz.client.dispatch.types import ComponentSelector
    from frequenz.client.common.microgrid.components import ComponentCategory

    from frequenz.channels import Receiver, Broadcast
    from unittest.mock import MagicMock

    class MyActor(Actor):
        def __init__(self, config_channel: Receiver[DispatchConfigurationEvent]):
            super().__init__()
            self._config_channel = config_channel
            self._dry_run: bool
            self._payload: dict[str, Any]

        async def _run(self) -> None:
            while True:
                config = await self._config_channel.receive()
                print("Received config:", config)

                self.set_components(config.components)
                self._dry_run = config.dry_run
                self._payload = config.payload

        def set_components(self, components: ComponentSelector) -> None:
            match components:
                case [int(), *_] as component_ids:
                    print("Dispatch: Setting components to %s", components)
                case [ComponentCategory.BATTERY, *_]:
                    print("Dispatch: Using all battery components")
                case _ as unsupported:
                    print(
                        "Dispatch: Requested an unsupported selector %r, "
                        "but only component IDs or category BATTERY are supported.",
                        unsupported,
                    )

    async def run():
        url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
        key  = os.getenv("DISPATCH_API_KEY", "some-key")

        microgrid_id = 1

        dispatcher = Dispatcher(
            microgrid_id=microgrid_id,
            server_url=url,
            key=key
        )

        # Create config channel to receive (re-)configuration events pre-start and mid-run
        config_channel = Broadcast[DispatchConfigurationEvent](name="config_channel")

        # Start actor and supporting actor, give each a config channel receiver
        my_actor = MyActor(config_channel.new_receiver())
        supporting_actor = MagicMock(config_channel.new_receiver())

        status_receiver = dispatcher.running_status_change.new_receiver()

        dispatch_handler = DispatchRunnerActor(
            actors=frozenset([my_actor, supporting_actor]),
            dispatch_type="EXAMPLE",
            running_status_receiver=status_receiver,
            configuration_sender=config_channel.new_sender(),
        )

        await asyncio.gather(dispatcher.start(), dispatch_handler.start())
    ```
    """

    def __init__(
        self,
        actors: frozenset[Actor],
        dispatch_type: str,
        running_status_receiver: Receiver[Dispatch],
        configuration_sender: Sender[DispatchConfigurationEvent] | None = None,
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actors: The actors to handle.
            dispatch_type: The type of dispatches to handle.
            running_status_receiver: The receiver for dispatch running status changes.
            configuration_sender: The sender for dispatch configuration events
        """
        super().__init__()
        self._dispatch_rx = running_status_receiver
        self._actors = actors
        self._dispatch_type = dispatch_type
        self._configuration_sender = configuration_sender

    def _start_actors(self) -> None:
        """Start all actors."""
        for actor in self._actors:
            if actor.is_running:
                _logger.warning("Actor %s is already running", actor.name)
            else:
                actor.start()

    async def _stop_actors(self, msg: str) -> None:
        """Stop all actors.

        Args:
            msg: The message to be passed to the actors being stopped.
        """
        for actor in self._actors:
            if actor.is_running:
                await actor.stop(msg)
            else:
                _logger.warning("Actor %s is not running", actor.name)

    async def _run(self) -> None:
        """Wait for dispatches and handle them."""
        while True:
            _logger.info("Waiting for dispatch...")
            dispatch = await self._dispatch_rx.receive()
            await self._handle_dispatch(dispatch=dispatch)

    async def _handle_dispatch(self, dispatch: Dispatch) -> None:
        """Handle a dispatch.

        Args:
            dispatch: The dispatch to handle.

        Returns:
            The running state.
        """
        running = dispatch.running(self._dispatch_type)
        match running:
            case RunningState.STOPPED:
                _logger.info("Stopping dispatch...")
                await self._stop_actors("Dispatch stopped")
            case RunningState.RUNNING:
                if self._configuration_sender is not None:
                    _logger.info("Updating configuration...")
                    await self._configuration_sender.send(
                        DispatchConfigurationEvent(
                            components=dispatch.selector,
                            dry_run=dispatch.dry_run,
                            payload=dispatch.payload,
                        )
                    )

                _logger.info("Running dispatch...")
                self._start_actors()
            case RunningState.DIFFERENT_TYPE:
                _logger.debug(
                    "Unknown dispatch! Ignoring dispatch of type %s", dispatch.type
                )
