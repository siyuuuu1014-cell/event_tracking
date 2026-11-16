from typing import Protocol

from ipaklink_analytics.domain.collection import PolicySnapshot


class PolicyUnavailable(Exception):
    """The service has no valid active policy to serve."""


class PolicyRepository(Protocol):
    async def get_active(self) -> PolicySnapshot:
        ...

