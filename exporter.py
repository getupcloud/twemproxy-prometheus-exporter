import os
import sys
import json
import socket
import prometheus_client as pc
from http.server import HTTPServer

pc.disable_created_metrics()

pc.REGISTRY.unregister(pc.GC_COLLECTOR)
pc.REGISTRY.unregister(pc.PLATFORM_COLLECTOR)
pc.REGISTRY.unregister(pc.PROCESS_COLLECTOR)

try:
    STATS_ENDPOINTS = [ (host or "127.0.0.1", int(port)) for host, port in [ ep.split(':') for ep in os.environ.get('STATS_ENDPOINTS', '').split(',') ]]
except ValueError as ex:
    print(f"Invalid var STATS_ENDPOINTS: {os.environ.get('STATS_ENDPOINTS', '')}")
    print("Format is STATS_ENDPOINTS=redis1-addr:port,redis2-addr:port,...")
    sys.exit(1)

HOST, PORT = os.environ.get("LISTEN_HOST", "0.0.0.0"), os.environ.get('LISTEN_PORT', 9090)

class DeltaCounter(pc.Counter):
    def set(self, value):
        cur_value = self._value.get()
        if cur_value == value:
            return
        self.inc(value - cur_value)

# Proxy Service
pc.TWEMPROXY_SERVICE_TOTAL_CONNECTIONS = DeltaCounter(
    'twemproxy_service_total_connections',
    'Total Connections to backend servers.',
    ['source', 'endpoint']
)

pc.TWEMPROXY_SERVICE_CURR_CONNECTIONS = pc.Gauge(
    'twemproxy_service_curr_connections',
    'Current Connections to backend servers.',
    ['source', 'endpoint']
)

# Pools
pc.TWEMPROXY_SERVER_POOL_CLIENT_EOF = DeltaCounter(
    'twemproxy_server_pool_client_eof',
    '# eof on client connections.',
    ['source', 'endpoint', 'pool_name']
)

pc.TWEMPROXY_SERVER_POOL_CLIENT_ERR = DeltaCounter(
    'twemproxy_server_pool_client_err',
    '# errors on client connections.',
    ['source', 'endpoint', 'pool_name']
)

pc.TWEMPROXY_SERVER_POOL_CLIENT_CONNECTIONS = pc.Gauge(
    'twemproxy_server_pool_client_connections',
    '# active client connections.',
    ['source', 'endpoint', 'pool_name']
)

pc.TWEMPROXY_SERVER_POOL_SERVER_EJECTS = DeltaCounter(
    'twemproxy_server_pool_server_ejects',
    '# times backend server was ejected.',
    ['source', 'endpoint', 'pool_name']
)

pc.TWEMPROXY_SERVER_POOL_FORWARD_ERROR = DeltaCounter(
    'twemproxy_server_pool_forward_error',
    '# times we encountered a forwarding error.',
    ['source', 'endpoint', 'pool_name']
)

pc.TWEMPROXY_SERVER_POOL_FRAGMENTS = DeltaCounter(
    'twemproxy_server_pool_fragments',
    '# fragments created from a multi-vector request.',
    ['source', 'endpoint', 'pool_name']
)

# Servers
pc.TWEMPROXY_SERVER_SERVER_EOF = DeltaCounter(
    'twemproxy_server_pool_server_eof',
    '# eof on server connections.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_SERVER_ERR = DeltaCounter(
    'twemproxy_server_pool_server_err',
    '# errors on server connections.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_SERVER_TIMEDOUT = DeltaCounter(
    'twemproxy_server_pool_server_timedout',
    '# timeouts on server connections.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_SERVER_CONNECTIONS = pc.Gauge(
    'twemproxy_server_pool_server_connections',
    '# active server connections.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_SERVER_EJECTED_AT = pc.Gauge(
    'twemproxy_server_pool_server_ejected_at',
    'timestamp when server was ejected in usec since epoch.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_REQUESTS = DeltaCounter(
    'twemproxy_server_pool_requests',
    '# requests.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_REQUEST_BYTES = DeltaCounter(
    'twemproxy_server_pool_request_bytes',
    'total request bytes.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_RESPONSES = DeltaCounter(
    'twemproxy_server_pool_responses',
    '# responses.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_RESPONSE_BYTES = DeltaCounter(
    'twemproxy_server_pool_response_bytes',
    'total response bytes.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_IN_QUEUE = pc.Gauge(
    'twemproxy_server_pool_in_queue',
    '# requests in incoming queue.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_IN_QUEUE_BYTES = pc.Gauge(
    'twemproxy_server_pool_in_queue_bytes',
    'current request bytes in incoming queue.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_OUT_QUEUE = pc.Gauge(
    'twemproxy_server_pool_out_queue',
    '# requests in outgoing queue.',
    ['source', 'endpoint', 'pool_name', 'server']
)

pc.TWEMPROXY_SERVER_OUT_QUEUE_BYTES = pc.Gauge(
    'twemproxy_server_pool_out_queue_bytes',
    'current request bytes in outgoing queue.',
    ['source', 'endpoint', 'pool_name', 'server']
)


proxy_stats = [
    "service",
    "source",
    "version",
    "uptime",
    "timestamp",
    "total_connections",
    "curr_connections",
    "endpoint",
]

pool_stats = [
    "client_eof",
    "client_err",
    "client_connections",
    "server_ejects",
    "forward_error",
    "fragments",
]

def load_stats():
    global STATS_PORTS
    for endpoint in STATS_ENDPOINTS:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(endpoint)
            stats = json.loads(str(sock.recv(8192), 'ascii')) # should be enough
            stats['endpoint'] = ':'.join(map(str, endpoint))
            yield stats

class RequestHandler(pc.MetricsHandler):
    def do_GET(self):
        for stats in load_stats():
            pc.TWEMPROXY_SERVICE_TOTAL_CONNECTIONS.labels(source=stats['source'], endpoint=stats['endpoint']).set(stats['total_connections'])
            pc.TWEMPROXY_SERVICE_CURR_CONNECTIONS.labels(source=stats['source'], endpoint=stats['endpoint']).set(stats['curr_connections'])
            for pool_name, pool in { k: v for k, v in stats.items() if k not in proxy_stats }.items():
                pc.TWEMPROXY_SERVER_POOL_CLIENT_EOF.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['client_eof'])
                pc.TWEMPROXY_SERVER_POOL_CLIENT_ERR.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['client_err'])
                pc.TWEMPROXY_SERVER_POOL_CLIENT_CONNECTIONS.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['client_connections'])
                pc.TWEMPROXY_SERVER_POOL_SERVER_EJECTS.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['server_ejects'])
                pc.TWEMPROXY_SERVER_POOL_FORWARD_ERROR.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['forward_error'])
                pc.TWEMPROXY_SERVER_POOL_FRAGMENTS.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name).set(pool['fragments'])
                for server_name, server in { k: v for k, v in pool.items() if k not in pool_stats }.items():
                    pc.TWEMPROXY_SERVER_SERVER_EOF.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['server_eof'])
                    pc.TWEMPROXY_SERVER_SERVER_ERR.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['server_err'])
                    pc.TWEMPROXY_SERVER_SERVER_TIMEDOUT.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['server_timedout'])
                    pc.TWEMPROXY_SERVER_SERVER_CONNECTIONS.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['server_connections'])
                    pc.TWEMPROXY_SERVER_SERVER_EJECTED_AT.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['server_ejected_at'])
                    pc.TWEMPROXY_SERVER_REQUESTS.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['requests'])
                    pc.TWEMPROXY_SERVER_REQUEST_BYTES.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['request_bytes'])
                    pc.TWEMPROXY_SERVER_RESPONSES.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['responses'])
                    pc.TWEMPROXY_SERVER_RESPONSE_BYTES.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['response_bytes'])
                    pc.TWEMPROXY_SERVER_IN_QUEUE.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['in_queue'])
                    pc.TWEMPROXY_SERVER_IN_QUEUE_BYTES.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['in_queue_bytes'])
                    pc.TWEMPROXY_SERVER_OUT_QUEUE.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['out_queue'])
                    pc.TWEMPROXY_SERVER_OUT_QUEUE_BYTES.labels(source=stats['source'], endpoint=stats['endpoint'], pool_name=pool_name, server=server_name).set(server['out_queue_bytes'])
        return super(RequestHandler, self).do_GET()

if __name__ == '__main__':
  print(f"Serving metrics from: {HOST}:{PORT}")
  print(f"Stats endpoints: {STATS_ENDPOINTS}")
  HTTPServer((HOST, PORT), RequestHandler).serve_forever()
