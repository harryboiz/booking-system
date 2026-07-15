# Ticket Event API

REST API CRUD cho event và nhận yêu cầu giữ vé bất đồng bộ, viết bằng Go. Dữ liệu
được lưu trong PostgreSQL, event được cache trên Redis và ticket được xử lý bất đồng
bộ theo batch bởi Kafka worker.

## Chạy project

Yêu cầu Go 1.23 trở lên, Docker và Docker Compose.

```bash
docker compose up -d postgres redis kafka kafka-init
export DATABASE_URL='postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable'
go run ./cmd/api
```

Server chạy tại `http://localhost:8080`. Kiểm tra bằng `GET /health`. Migration Goose
được embed và tự động chạy lệnh `up` khi ứng dụng khởi động.

Chạy worker ở terminal khác:

```bash
export DATABASE_URL='postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable'
go run ./cmd/worker
```

Chạy cancellation service ở terminal riêng:

```bash
export DATABASE_URL='postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable'
go run ./cmd/cancellation
```

## Configuration

`shared/config` load riêng từng file YAML:

- `config/services/api/config.local.yml`: địa chỉ HTTP server.
- `config/shared/postgres/config.local.yml`: kết nối PostgreSQL.
- `config/shared/redis/config.local.yml`: kết nối Redis.
- `config/shared/kafka/config.local.yml`: Kafka brokers và topic `ticket`.
- `config/services/worker/config.local.yml`: consumer group, message key/partition,
  batch tối đa 10.000 message, thời gian gom batch và timeout cancel.
- `config/services/cancellation/config.local.yml`: batch poll, chu kỳ poll và thời
  gian chờ trước khi tự động cancel ticket.

Config API dùng `includes` để mượn cấu hình PostgreSQL, với đường dẫn được tính từ
vị trí file API. Giá trị trong file API sẽ override giá trị được include nếu trùng key.
Model generator vẫn có thể đọc trực tiếp config PostgreSQL. Biến môi trường
`DATABASE_URL`, `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB`, `KAFKA_BROKERS` và
`KAFKA_TOPIC`, nếu được khai báo, sẽ override cấu hình tương ứng trong YAML.
Worker còn nhận `WORKER_GROUP_ID`, `WORKER_MESSAGE_KEYS` (danh sách phân tách bằng
dấu phẩy), `WORKER_BATCH_SIZE`, `WORKER_BATCH_WAIT` và `WORKER_CANCEL_AFTER`.
Cancellation service nhận `CANCELLATION_BATCH_SIZE`, `CANCELLATION_POLL_INTERVAL`
và `CANCELLATION_CANCEL_AFTER`.

Topic `ticket` có 100 partition. Kafka key là `event_id % 100` và được map trực tiếp
vào partition cùng số. Mỗi worker chỉ mở reader cho các partition liệt kê trong
`message_keys`; không cấu hình trùng một key cho nhiều worker đang chạy. Cấu hình
local mẫu nhận key 0–9. Với topic 3 partition cũ, cần tạo lại Kafka volume hoặc tăng
topic lên 100 partition trước khi chạy API/worker.

## Database migration

Migration dùng [Goose](https://github.com/pressly/goose) và được quản lý version
trong bảng `goose_db_version`.

- `migrations/001_create_events.sql`: tạo bảng `events` với lịch bắt đầu/kết thúc
  và các bộ đếm trạng thái ticket.
- `migrations/002_create_users.sql`: tạo bảng `users` và unique index cho email.
- `migrations/003_create_tickets.sql`: tạo bảng `tickets` với UUID do API sinh và
  cặp `(user_id, client_order_id)` unique; đồng thời tạo hypertable TimescaleDB
  `ticket_done` cùng các cột, partition theo `updated_at`. Các ràng buộc unique của
  `ticket_done` có thêm `updated_at` theo yêu cầu của TimescaleDB. Bảng `tickets` có
  partial index theo `created_at` để phục vụ cancellation service.

Cài Goose CLI:

```bash
go install github.com/pressly/goose/v3/cmd/goose@v3.24.3
```

Chạy migration bằng CLI:

```bash
goose -dir migrations postgres "$DATABASE_URL" status
goose -dir migrations postgres "$DATABASE_URL" up
goose -dir migrations postgres "$DATABASE_URL" down
```

## Generate database model

Model ánh xạ bảng không được viết tay. Sau khi PostgreSQL đang chạy và migration đã
được áp dụng, dùng `gorm.io/gen` để introspect schema và sinh lại model:

```bash
# 1. Khởi động PostgreSQL
docker compose up -d postgres

# 2. Cấu hình kết nối database
export DATABASE_URL='postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable'

# 3. Tạo/cập nhật schema trước khi generate
goose -dir migrations postgres "$DATABASE_URL" up

# 4. Generate model từ các cột thật của bảng events, users, tickets và ticket_done
go run ./tools/modelgen
```

Lệnh trên sinh model trong `shared/model/entity/`. DTO/validation của API nằm tại
`service/api/dto`. Không sửa trực tiếp file `*.gen.go`; sau mỗi lần thay đổi
migration/schema, áp dụng migration rồi chạy lại `go run ./tools/modelgen`.

## API

| Method | Endpoint | Mô tả |
| --- | --- | --- |
| `POST` | `/events` | Tạo event |
| `GET` | `/events` | Lấy danh sách event |
| `GET` | `/events/{id}` | Lấy một event |
| `PUT` | `/events/{id}` | Cập nhật toàn bộ event |
| `DELETE` | `/events/{id}` | Xóa event |
| `POST` | `/tickets/pending` | Giữ một vé trên Redis và gửi yêu cầu pending vào Kafka |
| `POST` | `/tickets/payment` | Tạo PayPal order và lấy URL thanh toán cho pending ticket |
| `POST` | `/tickets/confirm` | Xác nhận một pending ticket qua Kafka |

Ví dụ tạo event:

```bash
curl -i -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Go Conference",
    "description": "A conference for Go developers",
    "start_date": "2026-09-10T09:00:00+07:00",
    "end_time": "2026-09-10T18:00:00+07:00",
    "total_tickets": 200,
    "ticket_price": 49.5
  }'
```

`start_date` và `end_time` dùng RFC3339; `end_time` không được trước `start_date`.
Các cột stats chỉ đọc qua API và do worker quản lý.

### Tạo pending ticket

Redis lưu snapshot event theo key `events:{event_id}`. API đọc snapshot này và tính số
vé còn lại bằng `total_tickets - pending_tickets - confirm_tickets`.

Gọi API:

```bash
curl -i -X POST http://localhost:8080/tickets/pending \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": 10,
    "event_id": 1,
    "client_order_id": "order-20260715-0001"
  }'
```

API đọc Redis key `tickets:client-order-id:{user_id}:{client_order_id}`. Nếu key đã
trỏ tới một `order_id`, API trả ngay `202 Accepted` với `ticket_id` đó mà không kiểm
tra tồn kho hoặc publish lại. Nếu cache miss, API đọc event từ Redis để kiểm tra còn
vé nhưng không giảm tồn kho, sinh UUID rồi publish `UpdatedTicket` vào topic Kafka
`ticket` với key là `event_id % 100`. Sau khi publish thành công, API cập nhật ngay
mapping từ client order sang UUID; worker tiếp tục cập nhật tồn kho, snapshot pending
ticket và sửa lại cache khi xử lý hoặc reconcile.
Nếu hết vé, API trả `409 Conflict` với `{"error":"tickets sold out"}`.

### Lấy ticket

Truyền `user_id` và đúng một trong hai query parameter `ticket_id` hoặc
`client_order_id`:

```bash
curl -i 'http://localhost:8080/tickets?user_id=10&ticket_id=c7bca801-a080-45c9-972c-860cd4e44ab6'

curl -i 'http://localhost:8080/tickets?user_id=10&client_order_id=order-20260715-0001'
```

API ưu tiên đọc pending ticket từ Redis. Nếu không có pending ticket, API đọc bản
ghi mới nhất tương ứng trong bảng `ticket_done`. Ticket không tồn tại hoặc không
thuộc `user_id` trả `404 Not Found`.

### Tạo PayPal payment

```bash
curl -i -X POST http://localhost:8080/tickets/payment \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": 10,
    "ticket_id": "c7bca801-a080-45c9-972c-860cd4e44ab6"
  }'
```

API chỉ tạo PayPal order khi ticket tồn tại, thuộc user và còn `pending`. Response:

```json
{
  "paypal_order_id": "0f676fe2-8fb3-59f8-a40c-d1ac72ca51f5",
  "payment_url": "https://paypal.local/checkoutnow?token=0f676fe2-8fb3-59f8-a40c-d1ac72ca51f5"
}
```

PayPal simulator dùng `ticket_id` làm idempotency key và khóa thao tác tạo order.
Các request retry hoặc chạy đồng thời cho cùng một pending ticket luôn nhận cùng
`paypal_order_id` và `payment_url`; không tạo order thứ hai.

### Confirm ticket

```bash
curl -i -X POST http://localhost:8080/tickets/confirm \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": 10,
    "ticket_id": "c7bca801-a080-45c9-972c-860cd4e44ab6"
  }'
```

API đọc snapshot tại `tickets:{ticket_id}`. Ticket không tồn tại trả `404`; ticket
không thuộc user trả `403`; ticket không còn `pending` trả `409`. Ticket hợp lệ được
capture PayPal order đã tạo bởi `/tickets/payment`, sau đó publish lên Kafka với status
`confirm` và API trả `202 Accepted`. Nếu chưa tạo payment order, API trả `409`.

## Ticket worker

Khi khởi động, worker đọc các event còn hơn một ngày mới kết thúc và có
`event_id % 100` thuộc `message_keys`, rồi đồng bộ snapshot `events:{event_id}`, số
vé còn lại `tickets:reserved:{event_id}`, pending ticket và client-order mapping của
các event đó sang Redis.
Redis không phải source of truth; nếu Redis down hoặc cache miss, worker query
PostgreSQL và tiếp tục xử lý.

Worker gom tối đa 10.000 message, lấy ticket ID ở cả `tickets` và `ticket_done`, rồi
duyệt message theo thứ tự nhận:

- `pending`: insert vào `tickets` nếu ticket chưa tồn tại ở cả hai bảng.
- `confirm`: chỉ chuyển ticket `pending` từ `tickets` sang `ticket_done` với status
  `confirm`.
- `cancel`: chỉ chuyển ticket `pending` đã cũ từ 20 phút sang `ticket_done` với
  status `cancelled`.

Ticket và event stats của cả batch được ghi trong cùng một PostgreSQL transaction.
Sau commit, worker refresh Redis rồi mới commit Kafka offset. Pending order được cache
đầy đủ tại `tickets:{order_id}`. Khi order hoàn tất, worker xóa snapshot này nhưng giữ
key `tickets:client-order-id:{user_id}:{client_order_id}` trỏ tới done ticket ID.
Duplicate hoặc message không đúng state được bỏ qua, nên batch có thể retry theo cơ
chế at-least-once.

## Ticket cancellation service

Cancellation service là process độc lập với worker. Service chạy một lượt ngay khi
khởi động, sau đó mỗi `poll_interval` (local là 1 phút) poll toàn bộ bảng `tickets`,
không lọc theo `message_key`. Các ticket `pending` đã được tạo từ 20 phút trở lên
được publish lên Kafka với status `cancel`; worker consumer thực hiện việc chuyển
trạng thái. Mỗi lượt query tối đa `batch_size`; duplicate message vẫn an toàn nhờ
xử lý idempotent ở worker.

## Test

```bash
go test ./...
```

Unit test dùng fake repository cục bộ nên không yêu cầu PostgreSQL. Khi chạy ứng dụng,
`EventRepositoryImpl` là implementation PostgreSQL được sử dụng.

## Seed users

Thêm 100.000 user mẫu vào PostgreSQL (script tự chạy migration trước khi insert):

```bash
go run ./scripts/seed_users
```

Mặc định user dùng mật khẩu `password123`, được lưu dưới dạng bcrypt hash. Có thể đổi
mật khẩu và kích thước batch bằng biến môi trường/flags:

```bash
SEED_USER_PASSWORD='another-password' go run ./scripts/seed_users \
  -count 100000 -batch-size 1000
```

Mỗi lần chạy tạo một prefix email riêng nên có thể seed nhiều lần mà không trùng email.

## Cấu trúc source code

```text
cmd/
  api/                    # Binary entrypoint của API service
  worker/                 # Binary entrypoint của Kafka worker
service/
  api/
    routes.go             # Khai báo HTTP routes
    apierror/             # Tạo lỗi HTTP và tự ghi JSON error response
    apiresponse/          # Ghi JSON success response
    handler/              # HTTP handlers tách riêng theo resource (API, event)
    validation/           # Decode và validate request
    dto/                  # Request/response DTO
  worker/                 # Ticket message processor
config/
  services/api/          # Cấu hình local của API
  services/worker/       # Message keys, batch và timeout của worker
  shared/postgres/       # Cấu hình local của PostgreSQL
shared/
  config/                 # Loader dùng chung cho các file YAML trong config/
  database/               # Kết nối PostgreSQL và migration runner dùng chung
  kafka/                  # Kafka publisher và generic batch consumer
  model/entity/           # Entity do gorm.io/gen sinh từ bảng
  repository/             # Repository interface và lỗi dùng chung
    impl/                  # PostgreSQL repository implementation
migrations/               # Các file SQL migration độc lập với source code
tools/modelgen/           # Generator model từ schema PostgreSQL
scripts/seed_users/       # Seed user mẫu theo batch
```
