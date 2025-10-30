import asyncio
import concurrent.futures
import time
import traceback
from dataclasses import dataclass
from typing import Any

from flowrun.context import RunContext
from flowrun.task import TaskSpec


@dataclass
class ExecutionResult:
    """Result of executing a TaskSpec for a single attempt.

    Attributes
    ----------
    ok : bool
        True if the task completed successfully.
    result : Any
        The value returned by the task when successful.
    error : str | None
        The traceback string when the task failed, or None if there was no error.
    duration_s : float
        Execution duration in seconds.
    attempt : int
        The attempt number for this execution result (always 1 because tasks are executed once per DAG run).
    """

    ok: bool
    result: Any = None
    error: str | None = None
    duration_s: float = 0.0
    attempt: int = 1


class TaskExecutor:
    """
    SRP: run one TaskSpec exactly once.
    The scheduler is responsible for dependency ordering and state tracking.

    Uses:
    - asyncio for async tasks
    - ThreadPoolExecutor for sync IO tasks
    """

    def __init__(self, thread_pool: concurrent.futures.ThreadPoolExecutor) -> None:
        """Initialize the TaskExecutor.

        Parameters
        ----------
        thread_pool : concurrent.futures.ThreadPoolExecutor
            ThreadPoolExecutor used to run synchronous task functions.
        """
        self._thread_pool = thread_pool

    async def run_once(
        self,
        spec: TaskSpec,
        timeout_s: float | None,
        context: RunContext[Any] | None = None,
    ) -> ExecutionResult:
        """Execute a TaskSpec once, honoring an optional timeout and returning an ExecutionResult.

        Parameters
        ----------
        spec : TaskSpec
            The task specification to execute; may represent a synchronous function
            or an asynchronous coroutine function.
        timeout_s : float | None
            Maximum number of seconds to allow the task to run for this attempt,
            or None to disable the timeout.

        Returns
        -------
        ExecutionResult
            Object describing whether the task succeeded, the returned value (if any),
            the error traceback (if failed), the execution duration in seconds, and
            the attempt number.

        Notes
        -----
        Exceptions raised by the task are captured and returned in the ExecutionResult.error
        field; timeouts are translated into a failure ExecutionResult with an explanatory message.
        """
        start = time.time()
        try:
            if spec.requires_context and context is None:
                raise RuntimeError(
                    f"Task '{spec.name}' requires a RunContext but none was provided."
                )

            args: tuple[Any, ...] = ()
            if context is not None and spec.accepts_context:
                args = (context,)

            if spec.is_async():
                # run coroutine directly with timeout
                coro = spec.func(*args)
                res = await asyncio.wait_for(coro, timeout=timeout_s)
            else:
                # run sync func in thread pool with timeout
                loop = asyncio.get_running_loop()
                fut = loop.run_in_executor(self._thread_pool, spec.func, *args)
                res = await asyncio.wait_for(fut, timeout=timeout_s)

            return ExecutionResult(
                ok=True,
                result=res,
                duration_s=time.time() - start,
            )
        except TimeoutError:
            return ExecutionResult(
                ok=False,
                error=f"Timeout after {timeout_s}s",
                duration_s=time.time() - start,
            )
        except Exception as exc:
            return ExecutionResult(
                ok=False,
                error="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                duration_s=time.time() - start,
            )
