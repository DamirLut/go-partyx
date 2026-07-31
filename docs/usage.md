# Руководство по использованию partyx

partyx — каркас игрового сервера для быстрого прототипирования. Из коробки: бинарный WebSocket-транспорт ([arpack](https://github.com/edmand46/arpack)), аутентификация, сессии, RPC по опкодам, pub/sub-события, комнаты-акторы с типизированным состоянием (как в Colyseus), лобби. Вы пишете только: схему сообщений, аутентификатор, игровые команды и модули комнат.

## Сервер за 5 строк

Полный рабочий пример сервера (`go get github.com/damirlut/go-partyx`):

```go
app := partyx.New(partyx.Config{
    Addr:          ":8080",
    Authenticator: partyx.DevAuth(), // dev only, см. раздел 1
})

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
defer stop()
log.Fatal(app.Run(ctx)) // graceful shutdown по сигналу
```

`partyx.New` сам поднимает event bus, хранилище сессий, менеджер комнат, лобби, реестр команд и gateway, и регистрирует встроенные методы (`room.create`, `room.join`, `room.leave`, `lobby.list`; отключаются `DisableDefaultHandlers: true`).

Конфиг:

| Поле | По умолчанию | Назначение |
|------|--------------|-----------|
| `Addr` | `":8080"` | Адрес HTTP/WebSocket |
| `Authenticator` | `DevAuth()` + warning в лог | Проверка токена |
| `CheckOrigin` | `nil` (same-origin, безопасно) | Проверка Origin для WS |
| `DisableDefaultHandlers` | `false` | Выключить встроенные методы |

Доступы к подсистемам для расширенной обвязки: `app.Rooms()`, `app.Bus()`, `app.Commands()`, `app.Sessions()`, `app.Lobby()`, `app.Engine()` (gin — свои HTTP-роуты и middleware).

## 1. Свои сообщения и кодогенерация

Протокол бинарный: каждый WS-фрейм — одно arpack-сообщение. Свои типы описываете схемой (один Go-файл) и генерируете сериализаторы:

```go
// internal/messages/schema.go
package messages

//go:generate go run github.com/edmand46/arpack/cmd/arpack -in schema.go -out-go . -out-ts ../../web/src/messages

// Опкоды методов: >= 100 (0-99 зарезервированы фреймворком).
type GameOp uint16

const (
	GameOpGuess GameOp = 100
)

// Опкоды событий (сервер -> клиент): тоже >= 100.
type GameEvent uint16

const (
	GameEventWordGuessed  GameEvent = 100
	GameEventRoundStarted GameEvent = 101
)

type GuessRequest struct {
	Word string
}

type GuessResponse struct {
	Correct bool
}

type WordGuessed struct {
	PlayerID uint64
	Word     string
}

type RoundStarted struct {
	N uint32
}
```

Генерация: `go generate ./internal/messages` (или `make generate` в вашем проекте). Получаете `schema_gen.go` с методами `Marshal/Unmarshal` и — из той же схемы — TypeScript/C#/Lua биндинги для клиента (`-out-ts`, `-out-cs`, `-out-lua`). Сгенерированный код коммитится; инструмент пинится через `tools.go` (образец — `tools/tools.go` в этом репозитории).

Ограничения arpack касаются **только wire-типов**: целые явной ширины (`uint16`, не `int`), без указателей, мап и вложенных коллекций. Игровое **состояние** комнаты (раздел 4) сериализуется не обязано — там можно всё.

Валидация — методом `Validate() error` на типе запроса (в отдельном файле пакета, чтобы не затирался генерацией); фреймворк вызовет его автоматически и вернёт 400 при ошибке:

```go
// internal/messages/validate.go
package messages

import "errors"

func (m *GuessRequest) Validate() error {
	if m.Word == "" {
		return errors.New("word is required")
	}
	return nil
}
```

## 2. Аутентификация

Реализуйте интерфейс:

```go
type Authenticator interface {
	Authenticate(token string) (*session.Session, error)
}
```

Клиент первым сообщением шлёт `auth` с токеном; при ошибке — 401 и закрытие соединения, по таймауту (10 с) — тоже закрытие. Ответ — `AuthResult{SessionID, UserID}`. `Session.UserID` дальше используется везде: singleton-режимы комнат, `ctx.Session` в хендлерах. В `Session.Metadata` (`Get`/`Set`) — профиль, права и т.п.

`partyx.DevAuth()` — dev-вариант: токен = userID, свежая сессия на каждое подключение. Если `Authenticator` не задан вовсе, фреймворк подставляет DevAuth и пишет предупреждение в лог — в продакшене задавайте всегда явно.

## 3. Глобальные команды (RPC)

Глобальная команда регистрируется дженерик-хелпером — парсить payload руками не нужно:

```go
partyx.Handle(app, uint16(messages.GameOpGuess), func(ctx *partyx.Context, req *messages.GuessRequest) (*messages.GuessResponse, error) {
	// req уже декодирован и провалидирован (Validate, если есть)
	return &messages.GuessResponse{Correct: req.Word == "слово"}, nil
})
```

Что делает фреймворк:

1. Декодирует payload в `Req` (пустой payload → нулевой `Req`, так что методам без тела не нужен отдельный тип);
2. Вызывает `Validate()`, если `Req` его реализует (ошибка → 400);
3. Вызывает ваш хендлер и кодирует ответ ровно один раз (`nil`-ответ → пустой payload).

Ошибки: `return nil, partyx.NewError(400, "...")` — код уйдёт клиенту как есть; доменные ошибки комнат маппятся автоматически (404/409/410), остальное — 500.

`ctx` даёт:

| Поле | Что это |
|------|---------|
| `ctx.Session` | Сессия клиента (`UserID`, `Metadata`) |
| `ctx.ClientID` | ID соединения (игрок в комнатах) |
| `ctx.Bus` | EventBus — публикация событий |
| `ctx.Rooms` | Менеджер комнат (найти/создать комнату) |
| `ctx.Subscribe(topic)` / `ctx.Unsub(topic)` | Подписки клиента |

Escape hatch для сырых байт — `partyx.HandleRaw(app, op, fn)`. Опкод может иметь только одного владельца: дубликат (глобальный или из модуля комнаты) — паника при старте.

Хендлер вызывается из read-цикла соединения клиента — не делайте долгих блокирующих операций; игровую логику, привязанную к комнате, кладите в модуль комнаты (раздел 4), где она выполняется внутри актора.

## 4. Игровые модули комнат

Модуль = тип комнаты. Состояние игры живёт **внутри актора комнаты** и мутируется прямо в хуках и хендлерах — без мьютексов, каналов и `Do`. Тип описывается fluent-билдером: читается сверху вниз, цепочка заканчивается регистрацией:

```go
type WordState struct {
	Round int32
	Words map[uint64]string // server-only: мапы и указатели можно, это не wire-тип
}

partyx.Room[WordState]("wordgame"). // CreateRoomRequest.Type -> этот тип
	State(func() *WordState {
		return &WordState{Round: 1, Words: map[uint64]string{}}
	}).
	MaxPlayers(2).
	Singleton(protocol.SingletonReject).
	OnJoin(func(r *room.Room[WordState], p *room.Player) {
		r.State.Words[p.ID] = ""
	}).
	OnLeave(func(r *room.Room[WordState], p *room.Player) {
		delete(r.State.Words, p.ID)
	}).
	// Типизированный хендлер сообщения комнаты — выполняется внутри актора.
	Handle(uint16(messages.GameOpGuess), room.Typed(
		func(r *room.Room[WordState], p *room.Player, req *messages.GuessRequest) (*messages.GuessResponse, error) {
			correct := req.Word == "слово"
			if correct {
				r.State.Round++
				r.Broadcast(uint16(messages.GameEventWordGuessed),
					&messages.WordGuessed{PlayerID: p.ID, Word: req.Word})
			}
			return &messages.GuessResponse{Correct: correct}, nil
		})).
	Register(app)
```

Две особенности синтаксиса — это ограничения Go, а не прихоть API:

- тип состояния указывается явно — `partyx.Room[WordState]("wordgame")`: Go не выводит тип из последующего вызова `.State(...)`;
- типизированный хендлер оборачивается в `room.Typed(...)`: в Go нет дженерик-методов, поэтому `Handle` принимает сырой `room.MessageHandler[S]`, а `Typed` — дженерик-функция (декодирует payload, вызывает `Validate()`, кодирует ответ; `nil`-ответ → пустой payload).

Под капотом билдер собирает `room.Module` — внутреннюю непрозрачную структуру; её поля недоступны снаружи, так что внутреннее устройство может меняться без breaking changes.

Методы билдера (все опциональны, кроме `Register`):

| Метод | Когда/что |
|-------|-----------|
| `State(fn)` | Фабрика начального состояния (без неё — нулевое значение `S`) |
| `MaxPlayers(n)` | Дефолтный лимит игроков (0 = без лимита); клиент может переопределить при создании |
| `Singleton(mode)` | Singleton-режим; всегда из модуля, клиент переопределить не может |
| `OnInit(fn)` | Синхронно при создании комнаты (до публикации — детерминированно) |
| `OnJoin(fn)` | После добавления игрока (в том же сериализованном шаге) |
| `OnLeave(fn)` | После удаления игрока |
| `OnClose(fn)` | При удалении комнаты |
| `Tick(rate, fn)` | Игровой цикл: `fn` каждые `rate` внутри актора (таймеры, физика, раунды) |
| `Handle(op, h)` | Хендлер сообщения; паника при дубле опкода |
| `Register(app)` | Регистрация типа; паника при дубле типа или конфликте опкода с глобальным методом |

Что доступно внутри: `r.State` (ваше состояние), `r.Players()`, `r.Broadcast(op, msg)` / `r.BroadcastBytes`, `r.ID()`, `r.Config()`, `r.Close()`/`r.Open()`. Паника в хуке/хендлере логируется и не роняет актор.

**Маршрутизация room-scoped сообщений.** Клиент просто шлёт request с опкодом — без roomID. Фреймворк находит комнату так: опкод → тип модуля → комнаты клиента этого типа. Ровно одна — хендлер выполняется в ней; ни одной — 400 (`not in a room...`); несколько — 400 (`ambiguous`). Чтобы адресация всегда была однозначной, держите типы singleton (`reject`/`replace`) — это и есть типовой случай.

**Создание комнат.** Клиент шлёт `room.create` с `type: "wordgame"` — модуль даёт состояние и дефолтный конфиг (`SingletonMode` всегда из модуля; `name`/`maxPlayers` клиент может переопределить). Незарегистрированный тип создаёт «пустую» комнату без состояния — лобби и singleton-режимы работают и так.

**Жизненный цикл:** последний вышедший игрок удаляет комнату автоматически (`OnClose` → событие `room.removed` в `lobby`). Дисконнект клиента обрабатывается фреймворком (leave из всех комнат).

## 5. События

EventBus — pub/sub по строковым топикам; событие = опкод + уже закодированный payload (маршалится один раз на публикацию). Топики:

| Топик | Кто публикует |
|-------|---------------|
| `room:<id>` | Комната (`player.joined`/`player.left` + ваши игровые события через `r.Broadcast`) |
| `client:<id>` | Персональные сообщения клиенту (напр. `room.kicked`) |
| `lobby` | `room.created`, `room.removed` |

Клиент подписывается сам (сообщение `subscribe` с каналом) или через `ctx.Subscribe` в хендлере. Встроенные хендлеры подписывают клиента на топик комнаты *до* входа, поэтому он не пропускает события — делайте так же в своих.

Публикация в произвольный топик из любого места:

```go
ctx.Bus.Publish("lobby", eventbus.NewEvent(uint16(messages.GameEventTournamentStarted), &messages.TournamentStarted{}))
```

Свой подписчик (бот, метрики) реализует `Subscriber{ ID() uint64; Send(topic string, event Event) }`. Пустые топики удаляются автоматически; паника подписчика логируется и не роняет публикацию.

## 6. Сессии

`app.Sessions()` — потокобезопасное in-memory хранилище `id -> Session`. Gateway кладёт сессию при auth и удаляет при дисконнекте. `Session.Metadata` — потокобезопасная map для произвольных данных (`sess.Set("rating", 1450)`).

## 7. Wire-протокол (кратко)

Полностью — в `protocol/schema.go` и `docs/architecture.md`. Один endpoint `/ws`, бинарные фреймы `ClientMessage`/`ServerMessage`. Типы: `auth` / `subscribe` / `unsubscribe` / `request` от клиента; `response` / `error` / `event` от сервера. Коды ошибок: 400/401/404/409/410/500. Лимиты соединения: фрейм ≤ 64 КБ, auth-таймаут 10 с, ping 30 с / pong 60 с, буфер отправки 256 сообщений (переполнение = отключение медленного клиента), graceful close со сбросом очереди (2 с).

Клиентская сторона: сгенерируйте биндинги из `protocol/schema.go` + вашей схемы (`arpack -out-ts ...`) и гоняйте бинарные фреймы через `WebSocket.binaryType = "arraybuffer"`.

## 8. Продакшен-чеклист

- [ ] Свой `Authenticator` (JWT/OAuth/подпись), не `DevAuth`;
- [ ] `CheckOrigin: nil` (same-origin) или белый список доменов;
- [ ] `GIN_MODE=release` в окружении;
- [ ] Лимиты gateway пересмотрены под ваш трафик;
- [ ] In-memory хранилища (`session.Store`, `room.Manager`) заменены/обёрнуты, если нужна персистентность или несколько инстансов — за один процесс они не выйдут;
- [ ] Опкоды игры задокументированы в схеме (клиент генерируется из неё же — расхождений не будет);
- [ ] Graceful shutdown: `app.Run(ctx)` + `signal.NotifyContext` (как в примере выше).

## 9. Тестирование своего проекта

Образцы в репозитории:

- модульные: `room/module_test.go` (хуки, тик, типизированные хендлеры — актор тестируется синхронно: `Join`/`Leave`/`HandleMessage` детерминированы), `room/manager_test.go` (singleton-режимы, маршрутизация), `eventbus/eventbus_test.go`;
- интеграционный: `gateway/gateway_test.go` — поднимает сервер через `httptest.NewServer(gw.Engine())` и гоняет настоящий WebSocket-клиент бинарными фреймами по всему протоколу, включая room-scoped модуль (`TestRoomModuleFlow`). Копируйте этот паттерн для своих команд.
