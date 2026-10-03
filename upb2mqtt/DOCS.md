# upb2mqtt

Connects to a UPB PIM using [upb-lib](https://github.com/gwww/upb-lib), the
same library the Home Assistant UPB integration uses, and publishes devices
and links to MQTT with Home Assistant discovery.

Light states are published **retained**. When Home Assistant restarts it gets
every state from the broker right away, without re-reading every device over
the powerline. The bridge queries all devices only when the add-on itself
starts (about 1.4 s per device, less for devices that answer a single
transmission; see [Transmit count](#transmit-count)).

## Configuration

| Option | Description |
| --- | --- |
| `pim_url` | **Primary PIM (A)**, required. Serial device (`/dev/serial/by-id/usb-...`) or network PIM (`socket://10.0.5.3:2101`). |
| `pim_b_url` | **Secondary PIM (B)**, optional: a second PIM on the other powerline phase. Leave it empty for one PIM. See [Two PIMs](#two-pims). |
| `upe_file` | UPStart export. Your HA config folder is mounted read-only at `/homeassistant`. |
| `tx_count` | Send each UPB message this many times: `1`, `2`, or `auto` (default). See [Transmit count](#transmit-count). Overrides `tx_count` in the UPE file. |
| `keypad_sync` | Keep keypad LEDs in step with lights (default on). See [Keypad LEDs](#keypad-leds). |
| `keypad_scene_match` | `all` (default) or `any`: when a multi-light scene counts as on for its LED. |
| `batch_window` | Optional (under "Show unused optional configuration options"). Seconds to collect light commands before sending, default 0.15 (0 = off). See [Light groups](#light-groups). |
| `topic_prefix` | Default `upb`. |
| `discovery_prefix` | Default `homeassistant`. |
| `mqtt_*` | Only needed if you don't use the Mosquitto add-on. |
| `log_level` | `debug` also turns on upb-lib's protocol logging. |

upb-lib reads other flags (such as `use_raw_rate`) from the UPE file's `98,`
record, the same way the integration does. An export straight from UPStart
works as is.

## Entities

- **Light**, one per device channel, with on/off, brightness, transition
  (dimmable devices only) and flash. Keypads are skipped: they have no load,
  and their button presses appear as link events.
- **Scene**, one per link. Activating it activates the link.
- **Event**, one per link (`event.upb_link_<name>`). It fires `activated`,
  `deactivated`, `goto`, `blink`, `fade_started` or `fade_stopped` with
  `brightness_pct` and `rate` attributes, whether the link was triggered
  from HA or from a keypad. This replaces the `upb.scene_changed` bus event.
- **Button** "<link> deactivate", one per link. Disabled by default.
- **Button** *Refresh state* on each UPB device (diagnostic): re-reads the
  device's level over the powerline. Use it where you used
  `homeassistant.update_entity` with the UPB integration.
- **Sensor** *State changes* on each UPB device (diagnostic,
  `sensor.<device>_state_changes`): how often any of its lights changed
  state since the add-on started. Its attributes are the device's traffic
  over the same time, updated every minute:

  | Attribute | Meaning |
  | --- | --- |
  | `sent` | Commands the bridge sent to the device (not queries). |
  | `sent_scene` | Scene (link) commands the bridge sent that the device hears. |
  | `queries` | State reads the bridge sent. A single-transmission probe the device didn't answer while all devices are read (it's on the other PIM's phase) doesn't count. |
  | `observed` | Traffic from anyone else that concerns the device: a scene it hears (a keypad press), a command to it, or something it sent itself (a local press). Its answers to our queries don't count. |
  | `timeouts` | Queries it never answered. An unanswered query is sent once more; only when that goes unanswered too does it count. Misses of the single-transmission probes (see [Transmit count](#transmit-count)) are expected and don't count. |
  | `latency_ms` | Time from the PIM accepting the last state query to the device's answer; time waiting in the queue doesn't count. Only queries are timed, so it's updated when all devices are read (start, *Refresh device states*) and on *Refresh state*. |
  | `last_heard` | When the device last sent anything. |
  | `tx_count` | Transmit count in use for the device. |
  | `pim` | With two PIMs: the one that sends to it (A or B). |

  Counts start again at 0 when the add-on restarts. The sensor is
  `total_increasing`, so Home Assistant's statistics carry on across that.
- On the **UPB PIM** device: a *Connected* binary sensor, a *Refresh
  device states* button, and two diagnostic sensors read from the PIM's own
  registers every minute:
  - *Signal strength*: how strongly the PIM heard the last UPB message it
    received. It changes only when a message arrives.
  - *Noise floor*: powerline noise over the last 128 AC half-cycles, with
    the raw counts behind it as the `noise_counts` attribute.

  - *High noise*: a problem binary sensor, on while the noise floor is
    above 100.

  Both are raw PIM values (0–255, higher is stronger/noisier); PCS doesn't
  document their units. Reading them doesn't put anything on the powerline.

Entities become unavailable when the add-on or the PIM is disconnected
(with two PIMs, when both are). With two PIMs, the PIM device is named
**UPB PIM A**, and PIM B gets its own **UPB PIM B** device with the same
sensors.

## Device table

**Open Web UI** on the add-on's page shows every UPB device in one table:
its current level, the *State changes* statistics above and its transmit
count. Click a column heading to sort by it (again to reverse), and type in
the box to filter by name, product or unit ID. The table refreshes every 5
seconds and reads the bridge directly, so it's more current than the
sensors.

The refresh icon beside a device's name reads its state over the powerline
now, the same as its *Refresh state* button. It spins until the device
answers, then turns green, or red if the device didn't answer (the query
and its retry take up to about 10 s).

- To get it in Home Assistant's sidebar (as *UPB devices*), turn on **Show
  in sidebar** on the add-on's page.
- Each UPB device's page, and the UPB PIM's, has a **Visit** link to the
  table.

## Transmit count

Devices on the PIM's own powerline phase hear a message sent once. Others,
reached through the phase coupler or repeater, need it sent twice. A device
answers a query with as many copies as it was sent, so a query sent once
takes about 0.8 s instead of 1.4 s.

With `tx_count: auto` (the default) the bridge learns which devices answer
a single transmission each time it reads all devices: when it connects to the
PIM, and on *Refresh device states*. Nothing is kept across restarts, since
devices or the PIM may have moved.

- It queries each device once with a single transmission. A device that
  answers takes 1 transmission; the reply also gives its state. One that
  misses it (1.5 s timeout; replies take under 1 s) takes 2 and is read
  again that way. Whether a device hears a single transmission follows the
  leg (phase) it's on, so one answer is enough.
- Each query waits for the PIM's queue to be idle, so a light command waits
  behind at most one of them. Until a device has been queried, it gets 2.
- A scene (link) command goes out twice if any device in the scene, or any
  keypad showing it on an LED, needs two.
- A 1-transmission device that later misses a query goes to 2 until the next
  full read, starting with the retry of that query.
- The log lists the devices at each count after every full read, and each
  light's attributes include its `tx_count`.

A full read of about 65 devices with half of them on the far phase takes about
2 minutes, against about 1.5 minutes when every query is sent twice; after
that, every query and command to a near-phase device is faster. A light
command gets no reply, so a device that misses one isn't noticed until it
misses a query. Set `tx_count: 2` if that matters more.

## Two PIMs

With a PIM on each leg (phase) of a split-phase house, `pim_url` is the
primary PIM (A) and the optional `pim_b_url` is the secondary (B). Between
them, the two PIMs reach almost every device with a single transmission.
Both are assumed to reach every device with two.

- Only one PIM transmits at a time. Every message waits until neither PIM
  has one in flight, the last one has finished (the PIM reports it), and
  any further copies of a message just heard have passed. The two PIMs
  together send no faster than one; what they save is repeats.
- With `tx_count: auto`, a full read queries each device once from PIM A.
  A device that doesn't answer is queried once from PIM B. If that fails
  too, it's read from PIM A with two transmissions. A device on PIM A's leg
  is never queried from PIM B, and nothing PIM B answers is sent twice.
- The first read after the add-on starts waits up to 10 s for both PIMs to
  connect, so it doesn't send repeats to devices PIM B would reach once.
- Each message goes out on the connected PIM that reaches it with the fewest
  transmissions, PIM A on a tie. For a scene, that counts every device in
  it and every keypad showing it, so a scene with devices on both legs goes
  out twice from PIM A.
- If one PIM disconnects, everything goes through the other, twice except to
  devices it reaches once. When PIM B connects again, it's tried (once
  each) only for the devices PIM A doesn't reach once; nothing else is
  read.
- A message sent twice crosses to the other phase, so both PIMs hear it:
  each other's repeated messages, and a far device's repeated ones. A copy
  heard on both within a second, or one PIM's own message heard by the
  other, is handled once, so a scene fires one event. A message sent once
  stays on its phase and is never taken for a copy.
- Each light's attributes, its *State changes* sensor and the device table
  show the PIM (`pim`: A or B) beside its `tx_count`.
- With `tx_count: 1` or `2`, everything goes through PIM A and PIM B is a
  standby, used only while PIM A is disconnected.

## Keypad LEDs

A keypad toggle button sends "on" or "off" depending on whether its LED is
lit. The keypad changes its LEDs only when it hears a scene (link) command
listed in its indicator table. So when Home Assistant turns lights on or off
directly, the LEDs go stale and the next press seems to do nothing.

With `keypad_sync` on, the bridge keeps the LEDs in step:

- It reads each keypad's LEDs when it connects to the PIM (and on a refresh).
  With `tx_count: auto` that read is the keypad's transmit-count query:
  keypads are queried after all the lights, and no LED is corrected until
  every device has been read, so each LED is compared once against the full
  set of states.
- It tracks how the keypad reacts to the scene commands it hears, using the
  indicator table from the UPE file.
- When lights change, it works out what each LED should show from the same
  table: an LED is lit if any scene that turns it on is active. After a
  short pause it sets any LED that differs, with one Indicate command per
  LED. Button presses and scene activations need no extra traffic.

A scene is **active** when its lights match it: with `all`, every light it
turns on is on (at any level) and every light it turns off is off. With
`any`, at least one light it turns on is on. A scene that only turns lights
off is active when all of them are off, in both modes.

LEDs are left alone if any row that turns them on is a scene with no lights
in the UPE file (such as a gate status or keypad reset scene), or if a row
sets them to "opposite". The add-on log lists which LEDs each keypad syncs.
The last known LED states are published on `upb/keypad/<idx>/leds`.

## Light groups

Home Assistant turns a light group on or off by sending a command to each
light, which would be one powerline message per light. Instead the bridge
holds light commands for `batch_window` seconds. If a scene (link) in the UPE
file does what a set of them asks, it sends one link command for them:

- **activate** a scene when each of its lights is being set to that
  scene's level (off for lights the scene turns off);
- **deactivate** a scene when all of its lights are being turned off.

Every light in the scene must be in the batch, so the scene never changes a
light you didn't ask for. Bigger scenes are tried first, and several can be
used for one batch. Lights no scene covers get their own commands. Keypads
hear the scene command and update their LEDs as they would for a button
press.

A scene shown on a keypad, with at most 10 lights, can also stand in when
its other lights are already where it would put them. Turning off the last light that's on in an
"off" scene activates that scene. That's one message, and every keypad
showing the scene updates its own LEDs, instead of a light command plus an
Indicate per stale LED. The other lights must be known to be at the
scene's level (not unknown, and not mid-fade from a transition the bridge
started). When several scenes fit, the bridge picks the one covering the
most of the lights you commanded, then an activation over a deactivation,
then the scene with the fewest lights. The scene re-commands its other
lights, so one whose real level differed from what upb-lib recorded moves
to the scene's level, and the scene's event entity fires.

For example, with the Theater group's three lights turned off, the bridge
activates *Theater Off* instead of sending three commands. Turning the group
on sends three commands, since no scene turns on exactly those lights.

Only plain on/off/brightness commands are batched; commands with a
transition or flash are sent at once. A scene also fires its event entity
(`activated` / `deactivated`), just as when it is activated from HA.

Lights come on at the scene's fade rate rather than their default rate.

## Replacing the `upb.*` services

Send these with `mqtt.publish`. `<idx>` is shown as the `address` attribute
on each entity, for example `1_14_0` for a light and `1_17` for a link.

| Old service | Topic | Payload |
| --- | --- | --- |
| `upb.light_fade_start` | `upb/light/<idx>/cmd` | `{"command": "fade_start", "brightness_pct": 30, "rate": 10}` |
| `upb.light_fade_stop` | `upb/light/<idx>/cmd` | `{"command": "fade_stop"}` |
| `upb.light_blink` | `upb/light/<idx>/cmd` | `{"command": "blink", "blink_rate": 0.5}` |
| `homeassistant.update_entity` | press the device's *Refresh state* button | (or `upb/light/<idx>/cmd` `{"command": "update_status"}`) |
| `upb.link_deactivate` | `upb/link/<idx>/cmd` | `{"command": "deactivate"}` |
| `upb.link_goto` | `upb/link/<idx>/cmd` | `{"command": "goto", "brightness_pct": 60, "rate": 3}` |
| `upb.link_fade_start` | `upb/link/<idx>/cmd` | `{"command": "fade_start", "brightness_pct": 0, "rate": 30}` |
| `upb.link_fade_stop` | `upb/link/<idx>/cmd` | `{"command": "fade_stop"}` |
| `upb.link_blink` | `upb/link/<idx>/cmd` | `{"command": "blink", "blink_rate": 1}` |
| — | `upb/cmd` | `{"command": "sync"}` re-reads every device |

`brightness` (0–255) is accepted in place of `brightness_pct`. `rate` is in
seconds, or a raw UPB rate if the UPE file sets `use_raw_rate`. The default
`-1` means the device's own default rate.

```yaml
action: mqtt.publish
data:
  topic: upb/link/1_17/cmd
  payload: '{"command": "fade_start", "brightness_pct": 0, "rate": 30}'
```

## Migrating from the UPB integration

1. Delete the UPB integration (Settings → Devices & services). This frees
   its entity IDs.
2. Start the add-on. Discovery requests the same entity IDs the integration
   produced (`light.first_floor_office_main`, `scene.common_off`, ...), so
   dashboards and automations keep working.
3. Entity IDs you renamed by hand, and areas you assigned, belonged to the
   old integration's entities. Set them again on the new entities.
4. Automations that watched the old scenes' `last_change` attribute, or the
   `upb.scene_changed` event, should use an `event.received` trigger on the
   link's event entity (`event.upb_link_<name>`) with `event_type: activated`.

Requires Home Assistant 2025.10 or later (`default_entity_id` in discovery).

## Known limitations

This comes from upb-lib and behaves the same as in the integration.

- When a link is activated, device levels are *assumed* from the link
  table in the UPE file, not read back. The *Refresh device states* button
  corrects any drift.
