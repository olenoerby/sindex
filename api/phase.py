import logging
from contextlib import contextmanager

CURRENT_PHASE = 'Startup'


class PhaseFilter(logging.Filter):
    def filter(self, record):
        try:
            record.phase = CURRENT_PHASE
        except Exception:
            record.phase = 'unknown'
        return True


@contextmanager
def temp_phase(name: str):
    global CURRENT_PHASE
    prev = CURRENT_PHASE
    CURRENT_PHASE = name
    try:
        logging.getLogger().info(f"=== PHASE: {name} ===")
    except Exception:
        pass
    try:
        yield
    finally:
        CURRENT_PHASE = prev
        try:
            logging.getLogger().info(f"=== PHASE: {prev} ===")
        except Exception:
            pass


def attach_phase_filter(handler: logging.Handler):
    handler.addFilter(PhaseFilter())
