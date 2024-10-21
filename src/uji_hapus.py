import datetime
from functools import lru_cache, wraps


def get_ttl_hash(seconds=3600):
    """Calculate hash value for TTL caching.

    Args:
        seconds (int, optional): Expiration time in seconds. Defaults to 3600.

    Returns:
        int: Hash value.
    """
    utime = datetime.datetime.now().timestamp()
    return round(utime / (seconds + 1))


def ttl_cache(ttl_seconds=3600):
    """A decorator for TTL cache functionality.

    Args:
        ttl_seconds (int, optional): Expiration time in seconds. Defaults to 3600.
    """
    def ttl_cache_deco(func):
        """Returns a function with time-to-live (TTL) caching capabilities."""
        # Function with caching capability and dummy argument
        @lru_cache(maxsize=None)
        def cached_dummy_func(*args, ttl_dummy, **kwargs):
            del ttl_dummy  # Remove the dummy argument
            return func(*args, **kwargs)

        # Function to input the hash value into the dummy argument
        @wraps(func)
        def ttl_cached_func(*args, **kwargs):
            hash = get_ttl_hash(ttl_seconds)
            return cached_dummy_func(*args, ttl_dummy=hash, **kwargs)
        return ttl_cached_func

    return ttl_cache_deco


@ttl_cache(ttl_seconds=5)
def get_content():
    return "AAAAAAAAAAAAA"

print (get_content ())