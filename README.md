# Ticket Event API

REST API CRUD cho event và nhận yêu cầu giữ vé bất đồng bộ, viết bằng Go. Dữ liệu
được lưu trong PostgreSQL, số vé còn lại được giữ atomic trên Redis và yêu cầu pending
được publish vào Kafka.

## Chạy project

Yêu cầu Go 1.23 trở lên, Docker và Docker Compose.

```bash
docker compose up -d postgres redis kafka kafka-init
export DATABASE_URL='postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable'
go run ./cmd/api
```

Server chạy tại `http://localhost:8080`. Kiểm tra bằng `GET /health`. Migration Goose
được embed và tự động chạy lệnh `up` khi ứng dụng khởi động.

## Configuration

`shared/config` load riêng từng file YAML:

- `config/services/api/config.local.yml`: địa chỉ HTTP server.
- `config/shared/postgres/config.local.yml`: kết nối PostgreSQL.
- `config/shared/redis/config.local.yml`: kết nối Redis.
- `config/shared/kafka/config.local.yml`: Kafka brokers và topic `ticket`.

Config API dùng `includes` để mượn cấu hình PostgreSQL, với đường dẫn được tính từ
vị trí file API. Giá trị trong file API sẽ override giá trị được include nếu trùng key.
Model generator vẫn có thể đọc trực tiếp config PostgreSQL. Biến môi trường
`DATABASE_URL`, `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB`, `KAFKA_BROKERS` và
`KAFKA_TOPIC`, nếu được khai báo, sẽ override cấu hình tương ứng trong YAML.

## Database migration

Migration dùng [Goose](https://github.com/pressly/goose) và được quản lý version
trong bảng `goose_db_version`.

- `migrations/001_create_events.sql`: tạo bảng `events`.
- `migrations/002_create_users.sql`: tạo bảng `users` và unique index cho email.
- `migrations/003_create_tickets.sql`: tạo bảng `tickets` với UUID do API sinh và
  cặp `(user_id, client_order_id)` unique, liên kết user với event.

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

# 4. Generate model từ các cột thật của bảng events, users và tickets
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

Ví dụ tạo event:

```bash
curl -i -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Go Conference",
    "description": "A conference for Go developers",
    "date_time": "2026-09-10T09:00:00+07:00",
    "total_tickets": 200,
    "ticket_price": 49.5
  }'
```

`date_time` dùng định dạng RFC3339. `total_tickets` và `ticket_price` phải lớn hơn hoặc bằng 0.

### Tạo pending ticket

Redis lưu số vé có thể reserve theo key `tickets:reserved:{event_id}`. Ví dụ event 1
còn 100 vé:

```bash
docker compose exec redis redis-cli SET tickets:reserved:1 100
```

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

API dùng Redis `SETNX` để kiểm tra và giữ atomic key
`tickets:client-order-id:{user_id}:{client_order_id}`, rồi từ chối nếu key đã tồn tại.
Key được giữ lại để chặn retry, kể cả khi các bước tiếp theo thất bại. Sau đó API
kiểm tra Redis còn vé nhưng không giảm tồn kho, sinh UUID rồi publish `UpdatedTicket`
vào topic Kafka `ticket` với key là `event_id`. Kafka consumer chịu trách nhiệm giảm
tồn kho. API trả `202 Accepted` với `ticket_id`; nếu hết vé, API trả `409 Conflict`
với `{"error":"tickets sold out"}`.

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
service/
  api/
    routes.go             # Khai báo HTTP routes
    apierror/             # Tạo lỗi HTTP và tự ghi JSON error response
    apiresponse/          # Ghi JSON success response
    handler/              # HTTP handlers tách riêng theo resource (API, event)
    validation/           # Decode và validate request
    dto/                  # Request/response DTO
config/
  services/api/          # Cấu hình local của API
  shared/postgres/       # Cấu hình local của PostgreSQL
shared/
  config/                 # Loader dùng chung cho các file YAML trong config/
  database/               # Kết nối PostgreSQL và migration runner dùng chung
  model/entity/           # Entity do gorm.io/gen sinh từ bảng
  repository/             # Repository interface và lỗi dùng chung
    impl/                  # PostgreSQL repository implementation
migrations/               # Các file SQL migration độc lập với source code
tools/modelgen/           # Generator model từ schema PostgreSQL
scripts/seed_users/       # Seed user mẫu theo batch
```
