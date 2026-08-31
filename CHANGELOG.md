# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `RoomConfig.EmptyGrace` / `Module.EmptyGrace(d)` — keep an emptied room
  alive for the given duration before the manager removes it, so a
  disconnected player can reconnect into their seat. A join during the
  grace cancels the removal; a room still empty when it runs out is removed
  as before (the zero default keeps the immediate removal).

## [0.3.0] - 2026-08-31

### Added

- Opaque room creation options. `CreateRoomRequest.Options` carries
  key-value pairs (`CreateOption`) that partyx never interprets; the module
  reads them via `Room.Options()` (a direct accessor, safe inside the actor)
  or `RoomConfig.Options`. For module-backed rooms the request's options
  replace the module defaults as a whole; plain rooms store them as is.

## [0.2.0] - 2026-08-30

### Added

- `Room.Send(p *Player, op, msg)` — deliver a personal event to one player.
  The live connection is resolved by `p.UserID` inside the room actor, so a
  `*Player` captured before a reconnect still reaches the user's current
  connection; a user with no live player in the room is dropped silently.
- `Room.SendTo(userIDs, op, msg)` — the same delivery to a subset of room
  members, addressed by stable `userID` (e.g. an offer to two sides, a turn
  reminder). Unknown users are skipped.
- `Room.BroadcastExcept(except, op, msg)` — a room-wide event minus the
  listed users.
- `Room.BroadcastFunc(op, fn)` — per-player fan-out: `fn(p)` returns each
  player's own payload (hidden wallets, private hands); returning `false`
  skips the player.
- `Room.PlayerByUserID(userID)` — resolve the live player by stable user ID,
  for game state keyed by user.

### Behavior

- Personal sends publish to the target's `client:<id>` bus topic — the same
  channel the manager already uses for `room.kicked` — so other room members
  never see the payload and the gateway needed no changes.
- `Send`/`SendTo`/`BroadcastExcept` still encode the payload once per call;
  `BroadcastFunc` encodes per player by necessity.
- The five new methods run on the room actor and must be called from hooks,
  message handlers and `OnTick` (like `PlayerList`/`RoomInfo`);
  `Broadcast`/`BroadcastBytes` remain safe from anywhere.

## [0.0.1] - 2026-08-29

### Added

- Initial release of the partyx game server framework: WebSocket gateway
  (gorilla + gin, arpack binary protocol) with per-connection worker
  goroutines and a slow-consumer disconnect policy.
- Room actors: typed modules with state, lifecycle hooks, game loop
  (`Tick`), broadcast, presence events, singleton modes (`allow`/`reject`/
  `replace`) with atomic reconnect replacement (`JoinReplace`), and
  automatic removal of empty rooms.
- EventBus with `room:<id>`, `client:<id>` and `lobby` topics; typed and raw
  room message handlers; global command RPC.
- Sessions with pluggable `Authenticator` (dev-auth fallback), lobby listing,
  graceful shutdown of gateway, clients and room actors, configurable
  connection settings, structured logging with slog, consumer-owned gin
  engine with configurable WS path, CI and dependabot.

[0.3.0]: https://github.com/damirlut/go-partyx/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/damirlut/go-partyx/compare/v0.0.1...v0.2.0
[0.0.1]: https://github.com/damirlut/go-partyx/releases/tag/v0.0.1
