# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Explicit configuration boundary for vector storage unit tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from openviking.config.vector import VectorRuntimeSettings
from openviking.storage.viking_vector_index_backend import (
    VikingVectorIndexBackend as ProductionBackend,
)
from openviking_cli.utils.config.embedding_config import EmbeddingConfig


class ConfiguredVectorBackend(ProductionBackend):
    def __init__(self, config):
        super().__init__(config)
        embedding = EmbeddingConfig(
            dense={
                "provider": "openai",
                "api_key": "test",
                "model": "test",
                "dimension": config.dimension or 4,
            }
        )
        settings = VectorRuntimeSettings(embedding, config, "test", False)
        self.set_vector_config_resolver(
            SimpleNamespace(resolve=AsyncMock(return_value=settings))
        )
