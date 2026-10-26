# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Choose one complete Watch implementation and its matching URI coordinator."""

from typing import Any

from openviking.resource.uri_mutation_coordinator import UriMutationCoordinator
from openviking.resource.watch_scheduler import WatchScheduler


def build_watch_components(
    config: Any, *, resource_service: Any, viking_fs: Any,
    runtime_config_manager: Any = None,
) -> tuple[Any, Any]:
    provider = config.storage.watch_store.provider
    backend = config.watch_schedule_backend
    if provider == "file" and backend == "local":
        coordinator = UriMutationCoordinator()
        scheduler = WatchScheduler(
            resource_service=resource_service, viking_fs=viking_fs,
            uri_mutation_coordinator=coordinator, runtime_config_manager=runtime_config_manager,
        )
        return scheduler, coordinator
    if provider == "mysql" and backend == "redis":
        if config.storage.task_store.provider != "mysql":
            raise ValueError("Distributed Watch requires a MySQL Task store")
        from openviking.resource.distributed_uri_coordinator import DistributedUriCoordinator
        from openviking.resource.distributed_watch_scheduler import DistributedWatchScheduler

        store = config.storage.build_watch_store(viking_fs, service_name=config.service_name)
        return DistributedWatchScheduler(
            resource_service=resource_service, store=store, viking_fs=viking_fs,
            runtime_config_manager=runtime_config_manager,
        ), DistributedUriCoordinator(store)
    raise ValueError("Watch supports only file + local or mysql + redis")
