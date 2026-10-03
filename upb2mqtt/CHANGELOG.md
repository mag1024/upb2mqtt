# Changelog

## 0.8.0

- Removed the `refresh_hours` option and the periodic re-read of all
  devices. *Refresh device states* and each device's *Refresh state* still
  read devices on demand. A `refresh_hours` left in your configuration is
  ignored.
- `batch_window` is optional and no longer listed by default; it's under
  "Show unused optional configuration options" and still 0.15 s when unset.

## 0.7.2

- Configuration: *Secondary PIM (B), optional* is always listed (empty means
  one PIM), not hidden under "Show unused optional configuration options".

## 0.7.1

- Configuration: the PIM options are labelled *Primary PIM (A)* and
  *Secondary PIM (B), optional*.

## 0.7.0

- Optional second PIM (`pim_b_url`), for a PIM on each powerline leg. With
  `tx_count: auto`, a device PIM A doesn't reach with a single transmission
  is tried once from PIM B before falling back to two from PIM A, and each
  message goes out on the PIM that needs the fewest transmissions. Only
  one PIM transmits at a time. Either PIM keeps UPB working while the other
  is down; when PIM B comes back only its devices are tried again. A
  repeated message both PIMs hear is handled once. See *Two PIMs* in the
  documentation.
- PIM B has its own HA device (connected, signal strength, noise floor);
  with two PIMs, lights stay available while either PIM is connected
  (`<prefix>/powerline/status`). Light attributes, the stats sensor and the
  device table show each device's PIM.
- Each PIM has a *High noise* binary sensor (device class problem): on while
  its noise floor is above 100.

## 0.6.2

- `tx_count: auto`: each device is probed with one single-transmission query
  instead of two. Whether a device hears a single transmission follows the
  powerline leg it's on, so one answer is enough; a device that later misses
  a query still goes to 2. A full read of all devices is about 25 s shorter.

## 0.6.1

- Device table: a refresh icon beside each device's name reads its state
  now (like its *Refresh state* button). It spins until the device answers,
  then turns green, or red if it didn't answer.
- Fix: `timeouts` counted a query twice when upb-lib's retry of it was
  missed too, and counted a query answered on the retry. It now counts
  queries never answered.
- Fix (device table): rows showed through the Device heading and the
  totals row's first cell when scrolled.

## 0.6.0

- New diagnostic sensor *State changes* on each UPB device: how often its
  lights changed state since the add-on started. Its attributes count what
  the bridge sent to the device (commands, scene commands, queries), traffic
  from anyone else that concerns it (keypad presses, local presses, other
  controllers), and missed queries. They also give the last state query's
  latency, when the device was last heard, and its transmit count.
  Updated every minute on `upb/stats/<network>_<unit>` (retained).
- Device table: the add-on's web UI (ingress, and optionally the sidebar)
  shows every device's level and statistics in a table you can sort and
  filter. Each UPB device's *Visit* link opens it.

## 0.5.1

- `tx_count: auto`: keypads are no longer read separately at connect (at 2
  transmissions, with a retry). Their transmit-count query is their LED
  read, and one that misses it gets a single read at 2. Keypads are queried
  after the lights, and LED corrections wait until every device has been
  read.
- `tx_count: auto`: when a 1-transmission device misses a query, the retry
  of that query goes out twice too, instead of once more.
- Fix: a light change that arrived while the bridge was republishing its
  retained state (on connecting to the broker, or when Home Assistant came
  online) could be lost, or overwritten with the previous state, until that
  light changed again.
- Fix (`tx_count: auto`): reading all devices could stop at one device, until
  the next refresh or PIM reconnect, if the PIM rejected its
  single-transmission query or the device sent something else (a button
  press) while being queried. Such a query now counts as missed after 3 s.

## 0.5.0

- `tx_count` now takes `1`, `2` or `auto`, and defaults to `auto`. Each time
  the bridge reads all devices (connect, *Refresh device states*,
  `refresh_hours`) it queries every device with a single transmission first
  (1.5 s timeout) and sends to those that answer once: a query then takes
  about 0.8 s instead of 1.4 s. The others, and scenes containing one or
  shown on its keypads, get 2. Nothing is persisted. The log lists the
  devices at each count.
- Light attributes include `tx_count`.

## 0.4.0

- *Signal strength* and *Noise floor* diagnostic sensors on the UPB PIM
  device, read from the PIM's registers every minute. Published on
  `upb/pim/signal`.
- Light commands can also be replaced by a scene of at most 10 lights shown
  on a keypad when the scene's other lights are already at its levels, e.g.
  turning off the last light that's on activates the keypad's "off" scene.
  The keypads update their own LEDs, saving the Indicate commands.
- Scene substitution for light groups and keypad LED corrections now log at
  debug instead of info.

## 0.3.2

- Fix: 0.3.0 and 0.3.1 failed to start (`No module named 'scenes'`); the
  image now includes every module.

## 0.3.1

- New `tx_count` option (default 2): how many times each UPB message is
  sent. The UPE file no longer needs a `98,tx_count=2` record; if it has
  one, the option wins.

## 0.3.0

- Light groups: light commands arriving together (within `batch_window`,
  default 0.15 s) are replaced by a scene activate or deactivate where a
  scene sets exactly those lights. Fewer powerline messages, and keypad
  LEDs follow as they do for a button press.

## 0.2.0

- Keypad LED sync (`keypad_sync`, on by default): keypad LEDs, and so their
  toggle state, follow the lights their scenes control, including lights
  changed directly from Home Assistant. `keypad_scene_match` chooses whether
  a multi-light scene needs `all` (default) or `any` of its lights on.
- LED states are published on `upb/keypad/<idx>/leds`.

## 0.1.2

- Fix: scenes failed to activate from Home Assistant (`KeyError: 'payload_on'`);
  discovery now sends `payload_on` explicitly.
- Add a *Refresh state* button (diagnostic) to every UPB device, the MQTT
  replacement for `homeassistant.update_entity` on the old integration's lights.

## 0.1.1

- Fix: run without s6-overlay so the app sees SUPERVISOR_TOKEN and can
  get the Mosquitto broker credentials from the Supervisor.

## 0.1.0

- Initial release: upb-lib 0.7.2 bridged to MQTT with Home Assistant discovery.
