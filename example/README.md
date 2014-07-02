# Example: Node Distributed Task

Write to Redis list `new-task`:

```
$ telnet localhost 6379

LPUSH new-task '{"id": "<task_id1>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id2>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id3>", "function": "test", "data": "{\"manual_end\": true}"}'
LPUSH new-task '{"id": "<task_id4>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id5>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id6>", "function": "test", "data": "{\"manual_end\": true}"}'
LPUSH new-task '{"id": "<task_id7>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id8>", "function": "test", "data": "{\"manual_end\": true}"}'
LPUSH new-task '{"id": "<task_id9>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id10>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id11>", "function": "test", "data": "{}"}'
LPUSH new-task '{"id": "<task_id12>", "function": "test", "data": "{}"}'
```