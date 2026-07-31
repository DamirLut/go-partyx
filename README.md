# partyx

Игровой сервер-фреймворк на Go для быстрого прототипирования. Бинарный WebSocket-протокол на [arpack](https://github.com/edmand46/arpack) (кодогенерация, кросс-языковые клиенты: Go/TypeScript/C#/Lua), RPC по числовым опкодам, pub/sub события, комнаты-акторы с типизированным состоянием и игровым циклом (как в Colyseus), лобби.

Документация:
- [docs/usage.md](docs/usage.md) — как построить свою игру: фасад `partyx.App`, аутентификация, типизированные команды, игровые модули комнат, кодогенерация сообщений;
- [docs/architecture.md](docs/architecture.md) — внутреннее устройство: слои, протокол, конкурентность.

## Быстрый старт

```sh
go get github.com/damirlut/go-partyx
```

Минимальный сервер:

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	partyx "github.com/damirlut/go-partyx"
)

func main() {
	app := partyx.New(partyx.Config{
		Addr:          ":8080",
		Authenticator: partyx.DevAuth(), // dev only
		CheckOrigin:   func(r *http.Request) bool { return true }, // dev only
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	log.Fatal(app.Run(ctx))
}
```

Разработка библиотеки: `make build` / `make test` / `make vet` / `make lint`, `make generate` — перегенерировать `protocol` после правок `schema.go`.

Структура: корневой пакет `partyx` — фасад (App, Handle, Room-билдер, DevAuth); подпакеты — подсистемы (`protocol`, `gateway`, `command`, `eventbus`, `room`, `lobby`, `session`); внутренние методы по умолчанию — в `internal/handlers`; `tools` — пин кодогенератора arpack.

## Протокол

Один endpoint — `ws://localhost:8080/ws`. Каждый WS binary-фрейм — одно arpack-сообщение: `ClientMessage` от клиента, `ServerMessage` от сервера. Схема — `protocol/schema.go`, из неё же генерируются клиентские биндинги (TS/C#/Lua).

```
ClientMessage { type, id, op, token, channel, payload }
ServerMessage { type, id, code, op, channel, error, payload }
```

- `type`: auth / subscribe / unsubscribe / request от клиента; response / error / event от сервера;
- `op`: числовой опкод метода (у request) или события (у event); диапазон 0–99 зарезервирован за фреймворком, игровые опкоды — со 100;
- `payload`: arpack-байты конкретного типа сообщения.

Первым сообщением нужно пройти аутентификацию (`auth` с токеном), иначе соединение закрывается по таймауту (10 с).

## Встроенные методы

| Опкод | Метод | Payload запроса | Payload ответа |
|-------|-------|-----------------|----------------|
| 1 | `room.create` | `CreateRoomRequest` | `RoomInfo` |
| 2 | `room.join` | `JoinRoomRequest` | `RoomInfo` |
| 3 | `room.leave` | `LeaveRoomRequest` | — |
| 4 | `lobby.list` | — | `RoomList` |

Встроенные события: `player.joined` (1), `player.left` (2), `room.kicked` (3), `room.created` (4), `room.removed` (5) — см. `EventOp` в схеме.

`singletonMode`: `allow` (0) — без ограничений; `reject` (1) — ошибка 409, если уже в комнате этого типа; `replace` (2) — выкинуть старое подключение.

## Коды ошибок

| Код | Значение |
|-----|----------|
| 400 | Невалидный payload / формат сообщения / не в комнате для room-scoped опкода |
| 401 | Требуется аутентификация |
| 404 | Метод или комната не найдены |
| 409 | Комната заполнена / уже в комнате этого типа |
| 410 | Комната закрыта |
| 500 | Внутренняя ошибка |
