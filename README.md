# httpfile
lazily fetch a file using HTTP Range requests

See also pip's [lazy_wheel.py](https://github.com/pypa/pip/blob/main/src/pip/_internal/network/lazy_wheel.py) which applies this idea. Might not be 

These could both be improved by skipping the initial `HEAD` request, checking that the `Range` request succeeded, and parsing `Content-Length` from that response. pip's implementation in particular may have bugs fetching larger than its block/chunk size in one go.
