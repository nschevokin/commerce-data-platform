from __future__ import annotations

import psycopg

from commerce.build.candidate import FrontierNotReady
from commerce.contracts.manifest import ContractError
from commerce.ingestion.ledger import (
    ConcurrentWriteError,
    GenerationMovedError,
    GuardHeldError,
    StaleWriterError,
)


class RunConfigError(ContractError):
    pass


class InputNotReady(ContractError):
    pass


class DataInvalid(ContractError):
    pass


class QualityFailed(ContractError):
    pass


class VerificationFailed(RuntimeError):
    pass


class PipelineFailed(RuntimeError):
    pass


_CODES: tuple[tuple[type, str], ...] = (
    (InputNotReady, "INPUT_NOT_READY"), (FrontierNotReady, "FRONTIER_NOT_READY"),
    (DataInvalid, "DATA_INVALID"), (QualityFailed, "QUALITY_FAILED"),
    (VerificationFailed, "VERIFICATION_FAILED"), (RunConfigError, "RUN_CONFIG"),
    (StaleWriterError, "STALE_WRITER"), (GuardHeldError, "GUARD_HELD"),
    (GenerationMovedError, "GENERATION_MOVED"), (ConcurrentWriteError, "CONCURRENT_WRITE"),
    (ContractError, "CONTRACT"), (PipelineFailed, "PIPELINE_FAILED"),
    (psycopg.OperationalError, "DB_TRANSIENT"),
)


def error_code(exc: BaseException) -> str:
    for cls, code in _CODES:
        if isinstance(exc, cls):
            return code
    return "UNEXPECTED"


def is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, psycopg.OperationalError | GuardHeldError | ConcurrentWriteError | TimeoutError)
