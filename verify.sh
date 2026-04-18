#!/bin/bash
./bin/kproxy -broker localhost:9094 -listen :29094 -admin 127.0.0.1:9099 -topology 1=localhost:9094=127.0.0.1:29094 > /tmp/kproxy-verify.log 2>&1 &
PID=$!
sleep 2
echo "--- /healthz ---"
curl -s -w 'STATUS=%{http_code}\n' http://127.0.0.1:9099/healthz
echo "--- / (root) ---"
curl -s -w 'STATUS=%{http_code}\n' http://127.0.0.1:9099/
echo "--- /unknown (should be 404) ---"
curl -s -w 'STATUS=%{http_code}\n' http://127.0.0.1:9099/unknown
echo "--- run probe ---"
(cd example && go run ./probe 127.0.0.1:29094 2>&1 | tail -3)
echo "--- frames metric ---"
curl -s http://127.0.0.1:9099/metrics | grep -E '^kproxy_frames_total|^kproxy_intercepts_total|^kproxy_conn_active'
kill $PID 2>/dev/null
wait $PID 2>/dev/null
echo done
